/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.compiler.runtime.FieldWriter;
import org.objectweb.asm.Opcodes;

public class RuntimeWidgetGenerator extends UnitGenerator {
    public RuntimeWidgetGenerator(String name, ASMClassSource environment) {
        super(name, RuntimeWidget.class, environment);
    }

    public void adapt(TypeWidget targetType, RuntimeAdapter adapter) {
        //    public abstract Object property(Object source, String propertyName);
        //    public abstract Object index(Object source, Object index);
        //    public abstract void serializeJson(Object source, JsonGenerator generator);
        //    public abstract void mergeIntoFieldWriter(Object source, FieldWriter writer);
        generateProperty(targetType, adapter);
        generatePropertyDefault(targetType, adapter);
        generateIndex(targetType, adapter);

        generateMerge(targetType, adapter);
        generateGetFieldNames(targetType, adapter);

    }

    private void generateGetFieldNames(TypeWidget targetType, RuntimeAdapter adapter) {
        MethodGenerator method = createMethod("getFieldNames");
        method.setReturnType(new IterableTypeWidget(BaseTypeAdapter.STRING));
        BytecodeExpression sourceExpr = new BytecodeCastExpression(targetType, method.addArgument("source", AnyTypeWidget.getInstance()).read());
        if (!targetType.hasProperties()) {
            method.add(environment.constant(method.getReturnType(), ImmutableList.<String>of()));
        } else {
            method.add(targetType.getPropertyAdapter().getPropertyNameIterable(sourceExpr));
        }
        method.add(new ReturnCode(Opcodes.ARETURN));
    }

    private void generateProperty(TypeWidget targetType, RuntimeAdapter adapter) {
        MethodGenerator method = createMethod("property");
        method.setReturnType(AnyTypeWidget.getInstance());
        BytecodeExpression sourceExpr = new BytecodeCastExpression(targetType, method.addArgument("source", AnyTypeWidget.getInstance()).read());
        BytecodeExpression nameExpr = method.addArgument("name", BaseTypeAdapter.STRING).read();
        final BytecodeExpression resultExpr = adapter.property(sourceExpr, nameExpr);
        method.add(resultExpr);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.box(resultExpr.getType());
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
            }
        });
    }


    private void generatePropertyDefault(TypeWidget targetType, RuntimeAdapter adapter) {
        MethodGenerator method = createMethod("property");
        method.setReturnType(AnyTypeWidget.getInstance());
        BytecodeExpression sourceExpr = new BytecodeCastExpression(targetType, method.addArgument("source", AnyTypeWidget.getInstance()).read());
        BytecodeExpression nameExpr = method.addArgument("name", BaseTypeAdapter.STRING).read();
        BytecodeExpression defaultExpr = method.addArgument("def", AnyTypeWidget.getInstance()).read();
        final BytecodeExpression resultExpr = adapter.property(sourceExpr, nameExpr, defaultExpr);
        method.add(resultExpr);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.box(resultExpr.getType());
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
            }
        });
    }

    private void generateIndex(TypeWidget targetType, RuntimeAdapter adapter) {
        MethodGenerator method = createMethod("index");
        method.setReturnType(AnyTypeWidget.getInstance());
        BytecodeExpression sourceExpr = new BytecodeCastExpression(targetType, method.addArgument("source", AnyTypeWidget.getInstance()).read());
        BytecodeExpression nameExpr = method.addArgument("index", AnyTypeWidget.getInstance()).read();
        final BytecodeExpression resultExpr = adapter.index(sourceExpr, nameExpr);
        method.add(resultExpr);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.box(resultExpr.getType());
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
            }
        });
    }

    private void generateMerge(TypeWidget targetType, RuntimeAdapter adapter) {
        MethodGenerator method = createMethod("mergeIntoFieldWriter");
        BytecodeExpression sourceExpr = new BytecodeCastExpression(targetType, method.addArgument("source", AnyTypeWidget.getInstance()).read());
        BytecodeExpression generatorExpr = method.addArgument("index", environment.adaptInternal(FieldWriter.class)).read();
        method.add(adapter.mergeIntoFieldWriter(sourceExpr, generatorExpr));
        method.add(new ReturnCode());
    }
}
