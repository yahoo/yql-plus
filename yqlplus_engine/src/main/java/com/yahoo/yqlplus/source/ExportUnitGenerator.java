/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.source;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Provider;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.compiler.code.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.compiler.code.BaseTypeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.code.GambitScope;
import com.yahoo.yqlplus.compiler.code.ObjectBuilder;
import com.yahoo.yqlplus.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.internal.plan.ModuleType;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generate adapter class for Exports modules.
 */
public class ExportUnitGenerator extends SourceApiGenerator {
    public ExportUnitGenerator(GambitScope gambitScope) {
        super(gambitScope);
    }

    // source adapters can depend on the source & the context, but not the program

    public ObjectBuilder createModuleAdapter(String moduleName, Class<?> sourceClass, TypeWidget moduleType, TypeWidget contextType, BytecodeExpression sourceProvider) {
        gambitScope.addClass(sourceClass);
        ObjectBuilder adapter = gambitScope.createObject();
        ObjectBuilder.ConstructorBuilder cb = adapter.getConstructor();
        ObjectBuilder.FieldBuilder fld = adapter.field("$module", moduleType);
        fld.addModifiers(Opcodes.ACC_FINAL);
        cb.exec(fld.get(cb.local("this")).write(cb.cast(moduleType,
                cb.invokeExact(Location.NONE, "get", Provider.class, AnyTypeWidget.getInstance(), sourceProvider))));
        adapter.addParameterField(fld);
        return adapter;
    }


    class AdapterBuilder {
        String sourceName;
        Class<?> clazz;
        ObjectBuilder target;
        TypeWidget sourceClass;

        Multimap<String, ObjectBuilder.MethodBuilder> methods = Multimaps.newListMultimap(new TreeMap<String, Collection<ObjectBuilder.MethodBuilder>>(String.CASE_INSENSITIVE_ORDER), new com.google.common.base.Supplier<List<ObjectBuilder.MethodBuilder>>() {
            @Override
            public List<ObjectBuilder.MethodBuilder> get() {
                return Lists.newArrayList();
            }
        });

        Map<String, ObjectBuilder.MethodBuilder> fields = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);


        public AdapterBuilder(String moduleName, Class<? extends Exports> clazz, BytecodeExpression providerConstant) {
            this.clazz = clazz;
            this.sourceName = moduleName;
            // TODO: should accept the annotations for the free argument signatures, so they can be checked for @NotNullable or equiv.
            this.sourceClass = gambitScope.adapt(clazz, false);
            this.target = createModuleAdapter(moduleName, clazz, sourceClass, gambitScope.adapt(TaskContext.class, false), providerConstant);
        }


        public ModuleType create() {
            return new ExportModuleAdapter(target.type(), sourceName, methods, fields);
        }

        private void addAdapterMethod(final Method method) {
            ObjectBuilder.MethodBuilder adapterMethod = target.method(method.getName());
            TypeWidget contextType = gambitScope.adapt(TaskContext.class, false);
            adapterMethod.addArgument("$context", contextType);
            GambitCreator.ScopeBuilder block = adapterMethod.scope();

            TypeWidget outputType = gambitScope.adapt(method.getGenericReturnType(), true);
            AssignableValue resultValue = block.allocate(outputType);

            // construct for evaluate an expression in a context with or without timeout enforcement

            AssignableValue contextVar = block.local("$context");

            boolean isStatic = Modifier.isStatic(method.getModifiers());

            GambitCreator.Invocable targetMethod;
            if (isStatic) {
                targetMethod = block.findStaticInvoker(method.getDeclaringClass(), method.getName(), outputType, method.getParameterTypes());
            } else {
                targetMethod = block.findExactInvoker(method.getDeclaringClass(), method.getName(), outputType, method.getParameterTypes());
            }
            List<String> freeArgumentNames = Lists.newArrayList();
            Class<?>[] argumentTypes = method.getParameterTypes();
            java.lang.reflect.Type[] genericArgumentTypes = method.getGenericParameterTypes();
            Annotation[][] annotations = method.getParameterAnnotations();
            for (int i = 0; i < argumentTypes.length; ++i) {
                if (isFreeArgument(argumentTypes[i], annotations[i])) {
                    String name = gensym("arg$");
                    freeArgumentNames.add(name);
                    adapterMethod.addArgument(name, gambitScope.adapt(genericArgumentTypes[i], true));
                }
            }

            Iterator<String> freeArguments = freeArgumentNames.iterator();
            List<BytecodeExpression> invocationArguments = Lists.newArrayList();
            if (!isStatic) {
                invocationArguments.add(block.local("$module"));
            }
            
            
            visitMethodArguments(target, method, new ExportSourceArgumentVisitor(method), contextVar, block, freeArguments, invocationArguments, block);

            BytecodeExpression tracerExpr = block.propertyValue(Location.NONE, contextVar, "tracer");
            BytecodeExpression methodTracerExpr = block.invokeExact(Location.NONE, "start", Tracer.class, tracerExpr.getType(), tracerExpr, block.constant("PIPE"), block.constant(method.getDeclaringClass().getName() + "::" + method.getName()));

            BytecodeExpression invocation = targetMethod.invoke(Location.NONE, invocationArguments);
            block.set(Location.NONE, resultValue, invocation);
            for (BytecodeExpression argument:invocationArguments) {
                if (argument.getType().getJVMType().getClassName().equals(Tracer.class.getName())) {
                    block.exec(block.invokeExact(Location.NONE, "end", Tracer.class, BaseTypeAdapter.VOID, argument));                  
                }
            }
            block.exec(block.invokeExact(Location.NONE, "end", Tracer.class, BaseTypeAdapter.VOID, methodTracerExpr));
            adapterMethod.exit(block.complete(resultValue));
            methods.put(method.getName(), adapterMethod);
        }

        public void addExportedField(final Field field) {
            ObjectBuilder.MethodBuilder adapterMethod = target.method(gensym(field.getName() + "$get$"));
            TypeWidget contextType = gambitScope.adapt(TaskContext.class, false);
            adapterMethod.addArgument("$context", contextType);
            final TypeWidget outputType = gambitScope.adapt(field.getGenericType(), true);
            if (Modifier.isStatic(field.getModifiers())) {
                adapterMethod.exit(new BaseTypeExpression(outputType) {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.getMethodVisitor().visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(clazz), field.getName(), outputType.getJVMType().getDescriptor());
                    }
                });
            } else {
                adapterMethod.exit(new BaseTypeExpression(outputType) {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.exec(code.getLocal("$module"));
                        code.getMethodVisitor().visitFieldInsn(Opcodes.GETFIELD, Type.getInternalName(field.getDeclaringClass()), field.getName(), outputType.getJVMType().getDescriptor());
                    }
                });
            }
            fields.put(field.getName(), adapterMethod);
        }

        private class ExportSourceArgumentVisitor implements SourceArgumentVisitor {
            private final Method method;

            public ExportSourceArgumentVisitor(Method method) {
                this.method = method;
            }

            @Override
            public BytecodeExpression visitKeyArgument(Key key, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget) {
                reportMethodParameterException("Export", method, "@Export methods do not support @Key arguments: @Key('%s')", key.value());
                throw new IllegalArgumentException(); // reachability (reportMethodetc throws but javac doesn't know that
            }

            @Override
            public BytecodeExpression visitSet(Set annotate, DefaultValue defaultValue, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget) {
                reportMethodParameterException("Export", method, "@Export methods do not support @Set arguments: %s", annotate);
                throw new IllegalArgumentException(); // reachability (reportMethodetc throws but javac doesn't know that
            }
        }
    }

    public ModuleType apply(List<String> path, Provider<? extends Exports> input) {
        String sourceName = Joiner.on(".").join(path);
        Exports source = input.get();
        final Class<? extends Exports> clazz = source.getClass();
        final BytecodeExpression providerConstant = gambitScope.constant(input);
        AdapterBuilder builder = new AdapterBuilder(sourceName, clazz, providerConstant);
        for (Method method : clazz.getMethods()) {
            if (!method.isAnnotationPresent(Export.class)) {
                continue;
            }
            builder.addAdapterMethod(method);
        }

        for (Field field : clazz.getFields()) {
            if (!field.isAnnotationPresent(Export.class)) {
                continue;
            }
            builder.addExportedField(field);
        }

        return builder.create();
    }
}

