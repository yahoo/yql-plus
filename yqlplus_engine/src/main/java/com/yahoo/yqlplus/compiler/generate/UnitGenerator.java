/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;


import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.exprs.InvokeDynamicExpression;
import com.yahoo.yqlplus.compiler.runtime.Dynamic;
import com.yahoo.yqlplus.compiler.types.BaseTypeWidget;
import com.yahoo.yqlplus.compiler.types.ClosedPropertyAdapter;
import com.yahoo.yqlplus.compiler.types.PropertyAdapter;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.ConstructorGenerator;
import com.yahoo.yqlplus.compiler.code.MethodGenerator;
import org.objectweb.asm.*;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

import static org.objectweb.asm.Opcodes.*;

public class UnitGenerator {

    protected final String name;
    protected final String className;
    protected final String internalName;
    protected final String superInternalName;
    protected final String descriptor;
    protected final List<Type> interfaces;
    protected final Type jvmType;
    protected final ASMClassSource environment;
    protected final Annotatable annotations;
    protected TypeWidget typeWidget;

    public ASMClassSource getEnvironment() {
        return environment;
    }

    public final BytecodeSequence BASE_SUPER = new BytecodeSequence() {
        @Override
        public void generate(CodeEmitter mvE) {
            if (superInternalName != null) {
                mvE.getLocal("this").read().generate(mvE);
                MethodVisitor mv = mvE.getMethodVisitor();
                mv.visitMethodInsn(INVOKESPECIAL, superInternalName, "<init>", "()V", false);
            }
        }
    };

    private BytecodeSequence superInit = BASE_SUPER;


    protected final List<ConstructorGenerator> constructors = Lists.newArrayList();
    protected final Map<String, FieldDefinition> fields = Maps.newLinkedHashMap();
    protected final Map<String, FieldDefinition> superFields = Maps.newLinkedHashMap();
    protected final LinkedHashMultimap<String, MethodGenerator> methods = LinkedHashMultimap.create();

    public UnitGenerator(String name, ASMClassSource environment) {
        this(name, environment.getType(Object.class).getInternalName(), environment);
    }

    public UnitGenerator(String name, Class<?> superClazz, ASMClassSource environment) {
        this(name, environment.getType(superClazz).getInternalName(), environment);
        while (superClazz != Object.class) {
            readSuperFields(superClazz);
            superClazz = superClazz.getSuperclass();
        }
    }

    public UnitGenerator(String name, String superClassName, ASMClassSource environment) {
        this.name = name;
        this.environment = environment;
        if (name.contains(".")) {
            className = name;
        } else {
            className = "com.yahoo.yqlplus.generated." + environment.getUniqueElement() + "." + name;
        }
        this.internalName = className.replace('.', '/');
        this.superInternalName = superClassName.replace(".", "/");
        this.descriptor = "L" + internalName + ";";
        this.jvmType = Type.getType(descriptor);
        this.interfaces = Lists.newArrayList();
        this.annotations = new Annotatable(environment);
        this.typeWidget = new BaseTypeWidget(Type.getType("L" + internalName + ";")) {
            @Override
            public YQLCoreType getValueCoreType() {
                return YQLCoreType.OBJECT;
            }
            
            @Override
            public BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments) {
                final Type[] argTypes = new Type[arguments.size()];
                for (int i = 0; i < arguments.size(); ++i) {
                    argTypes[i] = arguments.get(i).getType().getJVMType();
                }
                // semi-gross that we always use invokedynamic here, fix later
                List<BytecodeExpression> fullArgs = Lists.newArrayListWithCapacity(arguments.size() + 1);
                fullArgs.add(target);
                fullArgs.addAll(arguments);
                return new InvokeDynamicExpression(Dynamic.H_BOOTSTRAP, "dyn:callMethod:" + methodName, outputType, fullArgs);
           } 

            @Override
            public PropertyAdapter getPropertyAdapter() {
                List<PropertyAdapter.Property> props = Lists.newArrayList();
                for (FieldDefinition field : Iterables.concat(superFields.values(), fields.values())) {
                    props.add(new PropertyAdapter.Property(field.getName(), field.getType()));
                }
                return new ClosedPropertyAdapter(this, props) {
                    private FieldDefinition getField(String propertyName) {
                        if (fields.containsKey(propertyName)) {
                            return fields.get(propertyName);
                        } else if (superFields.containsKey(propertyName)) {
                            return superFields.get(propertyName);
                        }
                        throw new PropertyNotFoundException("Property '%s' not found", propertyName);
                    }

                    @Override
                    protected AssignableValue getPropertyValue(BytecodeExpression target, String propertyName) {
                        return getField(propertyName).get(target);
                    }
                };
            }

            @Override
            public boolean hasProperties() {
                return !fields.isEmpty() || !superFields.isEmpty();
            }

            @Override
            public BytecodeExpression construct(BytecodeExpression... arguments) {
                if (arguments.length > 0) {
                    return invokeNew(getJVMType(), constructors, arguments);
                } else {
                    return super.construct(arguments);
                }
            }
        };
        environment.addUnit(this);
    }

    protected void readSuperFields(Class<?> superClazz) {
        String owner = Type.getInternalName(superClazz);
        for (Field field : superClazz.getFields()) {
            if (Modifier.isPublic(field.getModifiers())) {
                defineField(owner, environment.adaptInternal(field.getType()), field.getName());
            }
        }
    }


    protected TypeWidget getTypeWidget() {
        return typeWidget;
    }

    public ProgramValueTypeAdapter getValueTypeAdapter() {
        return environment.getValueTypeAdapter();
    }

    public void setTypeWidget(TypeWidget typeWidget) {
        this.typeWidget = typeWidget;
    }

    public Annotatable.AnnotationDefinition annotate(Type type) {
        return annotations.annotate(type);
    }

    public Annotatable.AnnotationDefinition annotate(Class<?> clazz) {
        return annotations.annotate(clazz);
    }

    public void addInterface(Class<?> clazz) {
        interfaces.add(environment.getType(clazz));
    }

    public FieldDefinition defineField(String owner, TypeWidget type, String name) {
        FieldDefinition superField = new FieldDefinition(environment, owner, type, name);
        superFields.put(name, superField);
        return superField;
    }

    public FieldDefinition createField(TypeWidget type, String name) {
        FieldDefinition field = new FieldDefinition(this, type, name);
        fields.put(name, field);
        return field;
    }

    public FieldDefinition getField(String name) {
        Preconditions.checkArgument(superFields.containsKey(name) || fields.containsKey(name), "Requested field '%s' does not exist", name);
        if (fields.containsKey(name)) {
            return fields.get(name);
        } else {
            return superFields.get(name);
        }
    }

    public FieldDefinition createField(String name, BytecodeExpression defaultValue) {
        FieldDefinition field = new FieldDefinition(this, name, defaultValue);
        fields.put(name, field);
        return field;
    }

    public MethodGenerator createMethod(String name) {
        MethodGenerator method = new MethodGenerator(this, name, false);
        this.methods.put(name, method);
        return method;
    }

    public MethodGenerator createStaticMethod(String name) {
        MethodGenerator method = new MethodGenerator(this, name, true);
        this.methods.put(name, method);
        return method;
    }

    public boolean hasMethod(String name) {
        return methods.containsKey(name);
    }

    //TODO: Currently this method is only called in the case only one MethodGenerator for the specific method name
    // Will add method to return whole set of MethodGenerators if necessary
    
    public MethodGenerator getMethod(String name) {
        Preconditions.checkArgument(methods.containsKey(name), "Requested method '%s' does not exist", name);
        return methods.get(name).iterator().next();
    }


    public ConstructorGenerator createConstructor() {
        ConstructorGenerator construct = new ConstructorGenerator(this);
        constructors.add(construct);
        return construct;
    }


    public void setSuperInit(BytecodeSequence superInit) {
        this.superInit = superInit;
    }

    public BytecodeSequence getSuperInit() {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                superInit.generate(code);

                for (Map.Entry<String, FieldDefinition> e : fields.entrySet()) {
                    e.getValue().generateInit(code);
                }
            }
        };
    }

    public String getSuperInternalName() {
        return superInternalName;
    }

    public BytecodeExpression constant(Class<?> declaredType, Object value) {
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(declaredType.isAssignableFrom(value.getClass()));
        return constant(environment.adaptInternal(declaredType), value);
    }

    public BytecodeExpression constant(Object value) {
        return constant(value.getClass(), value);
    }


    public BytecodeExpression constant(TypeWidget type, Object value) {
        return environment.constant(type, value);

    }

    public String getInternalName() {
        return internalName;
    }

    public void trace(OutputStream out) {
        TraceClassGenerator gen = new TraceClassGenerator();
        generate(gen);
        PrintStream pout = new PrintStream(out);
        for (StringWriter writer : gen.getWriters()) {
            pout.print(writer.toString());
        }
    }

    public void prepare(Class<?> clazz) {
        try {
            clazz.getField("$TYPE$").set(null, getType());
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new YQLTypeException("Unable to init $TYPE$ field", e);
        }
    }

    public void generate(ClassSink cws) {
        if (constructors.size() == 0) {
            createConstructor();
        }

        ClassVisitor cw = cws.create(internalName);
        String[] interfaceList = null;
        if (interfaces.size() > 0) {
            interfaceList = new String[interfaces.size()];
            for (int i = 0; i < interfaces.size(); ++i) {
                interfaceList[i] = interfaces.get(i).getInternalName();
            }
        }
        cw.visit(V1_7, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, internalName, null, superInternalName, interfaceList);

        annotations.generateAnnotations(cw);
        {
            FieldVisitor fv = cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$TYPE$", Type.getDescriptor(TypeWidget.class), null, null);
            fv.visitEnd();
        }

        for (Map.Entry<String, FieldDefinition> e : fields.entrySet()) {
            e.getValue().generate(cw);
        }

        for (ConstructorGenerator constructor : constructors) {
            constructor.generate(cw);
        }

        for (Map.Entry<String, MethodGenerator> e : methods.entries()) {
            e.getValue().generate(cw);
        }

        cw.visitEnd();
    }

    public Class<?> getGeneratedClass() throws ClassNotFoundException {
        return environment.getGeneratedClass(this);
    }

    public final TypeWidget getType() {
        return typeWidget;
    }

}
