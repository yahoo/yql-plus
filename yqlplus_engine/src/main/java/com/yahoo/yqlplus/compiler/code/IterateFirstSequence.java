/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Iterator;

class IterateFirstSequence implements BytecodeExpression {
    private final BytecodeExpression target;
    private final TypeWidget valueType;

    public IterateFirstSequence(BytecodeExpression target, TypeWidget valueType) {
        this.target = target;
        this.valueType = NullableTypeWidget.create(valueType.boxed());
    }

    @Override
    public TypeWidget getType() {
        return valueType;
    }

    @Override
    public void generate(CodeEmitter start) {
        CodeEmitter code = start.createScope();
        Label done = new Label();
        Label isNull = new Label();
        MethodVisitor mv = code.getMethodVisitor();
        BytecodeExpression tgt = code.evaluateOnce(target);
        tgt.generate(code);
        code.emitInstanceCheck(tgt.getType(), Iterable.class, isNull);
        AssignableValue iterator = code.allocate(Iterator.class);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterable.class), "iterator", Type.getMethodDescriptor(Type.getType(Iterator.class)), true);
        code.exec(iterator.write(code.adapt(Iterator.class)));
        code.exec(iterator.read());
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterator.class), "hasNext", Type.getMethodDescriptor(Type.BOOLEAN_TYPE), true);
        mv.visitJumpInsn(Opcodes.IFEQ, isNull);
        code.exec(iterator.read());
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterator.class), "next", Type.getMethodDescriptor(Type.getType(Object.class)), true);
        code.cast(valueType, AnyTypeWidget.getInstance(), isNull);
        mv.visitJumpInsn(Opcodes.GOTO, done);
        mv.visitLabel(isNull);
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitLabel(done);
    }
}
