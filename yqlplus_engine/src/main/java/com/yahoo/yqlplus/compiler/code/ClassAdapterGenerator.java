/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.Map;

public class ClassAdapterGenerator extends UnitGenerator {
    public ClassAdapterGenerator(String name, ASMClassSource environment) {
        super(name, environment);
    }

    public void generateAdapater(TypeWidget targetType, final PropertyAdapter propertyAdapter) {
        Preconditions.checkArgument(propertyAdapter.isClosed(), "ClassAdapter only works on closed types");
        generateGetProperty(targetType, propertyAdapter);
        generatePutProperty(targetType, propertyAdapter);
    }

    private void generateGetProperty(TypeWidget targetType, final PropertyAdapter propertyAdapter) {
        MethodGenerator method = createStaticMethod("getProperty");
        method.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression targetExpr = method.addArgument("target", targetType).read();
        final BytecodeExpression propertyName = method.addArgument("property", BaseTypeAdapter.STRING).read();
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Map<String, Label> labelMap = Maps.newLinkedHashMap();
                Label defaultCase = new Label();
                Label done = new Label();
                for (PropertyAdapter.Property property : propertyAdapter.getProperties()) {
                    labelMap.put(property.name, new Label());
                }
                propertyName.generate(code);
                code.emitStringSwitch(labelMap, defaultCase, true);
                MethodVisitor mv = code.getMethodVisitor();
                for (PropertyAdapter.Property property : propertyAdapter.getProperties()) {
                    mv.visitLabel(labelMap.get(property.name));
                    propertyAdapter.property(targetExpr, property.name).read().generate(code);
                    code.box(property.type);
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                }
                mv.visitLabel(defaultCase);
                new NullExpr(AnyTypeWidget.getInstance()).generate(code);
                mv.visitLabel(done);
                mv.visitInsn(Opcodes.ARETURN);
            }
        });
    }

    private void generatePutProperty(TypeWidget targetType, final PropertyAdapter propertyAdapter) {
        MethodGenerator method = createStaticMethod("putProperty");
        final BytecodeExpression targetExpr = method.addArgument("target", targetType).read();
        final BytecodeExpression propertyName = method.addArgument("property", BaseTypeAdapter.STRING).read();
        final BytecodeExpression valueExpr = method.addArgument("value", AnyTypeWidget.getInstance()).read();
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Map<String, Label> labelMap = Maps.newLinkedHashMap();
                Label done = new Label();
                for (PropertyAdapter.Property property : propertyAdapter.getProperties()) {
                    labelMap.put(property.name, new Label());
                }
                propertyName.generate(code);
                code.emitStringSwitch(labelMap, done, true);
                MethodVisitor mv = code.getMethodVisitor();
                for (PropertyAdapter.Property property : propertyAdapter.getProperties()) {
                    mv.visitLabel(labelMap.get(property.name));
                    AssignableValue av = propertyAdapter.property(targetExpr, property.name);
                    try {
                        code.exec(av.write(new BytecodeCastExpression(property.type, valueExpr)));
                    } catch (UnsupportedOperationException ignored) {
                        // can't write what isn't writable
                    }
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                }
                mv.visitLabel(done);
                mv.visitInsn(Opcodes.RETURN);
            }
        });
    }
}
