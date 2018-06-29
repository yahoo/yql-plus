/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.api.types.YQLNamePair;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.compiler.exprs.BytecodeCastExpression;
import com.yahoo.yqlplus.compiler.runtime.StructBase;
import com.yahoo.yqlplus.compiler.types.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import com.yahoo.yqlplus.compiler.types.ClosedPropertyAdapter;
import com.yahoo.yqlplus.compiler.types.FieldAssignableValue;
import com.yahoo.yqlplus.compiler.types.PropertyAdapter;
import com.yahoo.yqlplus.compiler.types.StructBaseTypeWidget;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.compiler.exprs.ReturnCode;
import com.yahoo.yqlplus.compiler.exprs.NullExpr;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;
import java.util.Map;

public class StructGenerator extends UnitGenerator {
    private final Map<String, FieldProperty> fields;
    private final List<PropertyAdapter.Property> propertyList;


    class FieldProperty {
        PropertyAdapter.Property property;

        FieldProperty(String name, TypeWidget type) {
            this.property = new PropertyAdapter.Property(name, type);
        }

        FieldAssignableValue property(BytecodeExpression target) {
            return new FieldAssignableValue(getInternalName(), property.name, property.type, target);
        }
    }


    public StructGenerator(String name, ASMClassSource environment) {
        super(name, StructBase.class, environment);
        this.fields = Maps.newLinkedHashMap();
        this.propertyList = Lists.newArrayList();
    }

    public void define(YQLStructType valueType) {
        Preconditions.checkArgument(valueType.isClosed(), "Create classes only for closed types");
        for (YQLNamePair field : valueType.getFields()) {
            TypeWidget type = environment.adapt(field.getValueType());
            final FieldProperty fieldProperty = new FieldProperty(field.getName(), type);
            fields.put(field.getName(), fieldProperty);
            propertyList.add(new PropertyAdapter.Property(field.getName(), type));
            createField(fieldProperty.property.type, fieldProperty.property.name);
        }

        setTypeWidget(new StructTypeWidget(getType().getJVMType()));
        implementRecord();
    }

    public void addField(String fieldName, TypeWidget type) {
        final FieldProperty fieldProperty = new FieldProperty(fieldName, type);
        this.fields.put(fieldName, fieldProperty);
        propertyList.add(new PropertyAdapter.Property(fieldName, type));
        createField(type, fieldName);
    }

    public TypeWidget build() {
        implementRecord();
        setTypeWidget(new StructTypeWidget(getType().getJVMType()));
        return getType();
    }

    private void implementRecord() {
        MethodGenerator fieldNamesGenerator = createMethod("getAllFieldNames");
        ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
        fieldNames.addAll(fields.keySet());
        BytecodeExpression expr = constant(fieldNames.build());
        fieldNamesGenerator.setReturnType(getValueTypeAdapter().adaptInternal(new TypeLiteral<Iterable<String>>() {
        }));
        fieldNamesGenerator.add(expr);
        fieldNamesGenerator.add(new ReturnCode(Opcodes.ARETURN));

        //   Object get(String field);
        generateGetMethod();

        //   void put(String field, Object value);
        generatePutMethod();

        // generate equals() and hashCode and hashValue(byte[] ...)
    }

    private void generatePutMethod() {
        final MethodGenerator setGenerator = createMethod("set");
        final BytecodeExpression propertyNameExpr = setGenerator.addArgument("propertyName", BaseTypeAdapter.STRING).read();
        final BytecodeExpression valueExpr = setGenerator.addArgument("value", AnyTypeWidget.getInstance()).read();
        setGenerator.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Map<String, Label> labelMap = Maps.newLinkedHashMap();
                Label done = new Label();
                for (Map.Entry<String, FieldProperty> field : fields.entrySet()) {
                    labelMap.put(field.getKey(), new Label());
                }
                propertyNameExpr.generate(code);
                code.emitStringSwitch(labelMap, done, true);
                MethodVisitor mv = code.getMethodVisitor();
                for (Map.Entry<String, FieldProperty> field : fields.entrySet()) {
                    mv.visitLabel(labelMap.get(field.getKey()));
                    BytecodeExpression castValue = new BytecodeCastExpression(field.getValue().property.type, valueExpr);
                    field.getValue().property(setGenerator.getLocal("this").read()).write(castValue).generate(code);
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                }
                mv.visitLabel(done);
                mv.visitInsn(Opcodes.RETURN);
            }
        });
    }

    private void generateGetMethod() {
        final MethodGenerator getGenerator = createMethod("get");
        getGenerator.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression propertyNameExpr = getGenerator.addArgument("propertyName", BaseTypeAdapter.STRING).read();
        getGenerator.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Map<String, Label> labelMap = Maps.newLinkedHashMap();
                Label defaultCase = new Label();
                Label done = new Label();
                for (Map.Entry<String, FieldProperty> field : fields.entrySet()) {
                    labelMap.put(field.getKey(), new Label());
                }
                propertyNameExpr.generate(code);
                code.emitStringSwitch(labelMap, defaultCase, true);
                MethodVisitor mv = code.getMethodVisitor();
                for (Map.Entry<String, FieldProperty> field : fields.entrySet()) {
                    mv.visitLabel(labelMap.get(field.getKey()));
                    field.getValue().property(getGenerator.getLocal("this").read()).read().generate(code);
                    code.box(field.getValue().property.type);
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                }
                mv.visitLabel(defaultCase);
                new NullExpr(AnyTypeWidget.getInstance()).generate(code);
                mv.visitLabel(done);
                mv.visitInsn(Opcodes.ARETURN);
            }
        });
    }

    class StructPropertyAdapter extends ClosedPropertyAdapter {

        StructPropertyAdapter(TypeWidget type, Iterable<Property> properties) {
            super(type, properties);
        }

        private FieldProperty getFieldProperty(String propertyName) {
            FieldProperty property = fields.get(propertyName);
            if (property == null) {
                throw new PropertyNotFoundException("Struct object '%s' does not have property '%s'", getType().getJVMType().getDescriptor(), propertyName);
            }
            return property;
        }

        @Override
        protected AssignableValue getPropertyValue(BytecodeExpression target, String propertyName) {
            return getFieldProperty(propertyName).property(target);
        }
    }

    class StructTypeWidget extends StructBaseTypeWidget {
        StructTypeWidget(Type type) {
            super(type);
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PropertyAdapter getPropertyAdapter() {
            return new StructPropertyAdapter(this, propertyList);
        }
    }

}
