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

class IterateSequence implements BytecodeSequence {
    private final BytecodeExpression target;
    private final AssignableValue item;
    private final TypeWidget valueType;
    private final IterateAdapter.IterateLoop loop;

    public IterateSequence(BytecodeExpression target, TypeWidget valueType, IterateAdapter.IterateLoop loop) {
        this.target = target;
        this.valueType = valueType;
        this.loop = loop;
        this.item = null;
    }

    public IterateSequence(BytecodeExpression target, TypeWidget valueType, AssignableValue item, IterateAdapter.IterateLoop loop) {
        this.target = target;
        this.valueType = valueType;
        this.loop = loop;
        this.item = item;
    }

    @Override
    public void generate(CodeEmitter code) {
        Label done = new Label();
        Label next = new Label();
        MethodVisitor mv = code.getMethodVisitor();
        BytecodeExpression tgt = code.evaluateOnce(target);
        code.exec(tgt);
        code.emitInstanceCheck(tgt.getType(), Iterable.class, done);
        AssignableValue item = this.item == null ? code.allocate(valueType) : this.item;
        AssignableValue iterator = code.allocate(Iterator.class);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterable.class), "iterator", Type.getMethodDescriptor(Type.getType(Iterator.class)), true);
        code.exec(iterator.write(code.adapt(Iterator.class)));
        mv.visitLabel(next);
        code.exec(iterator.read());
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterator.class), "hasNext", Type.getMethodDescriptor(Type.BOOLEAN_TYPE), true);
        mv.visitJumpInsn(Opcodes.IFEQ, done);
        code.exec(iterator.read());
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterator.class), "next", Type.getMethodDescriptor(Type.getType(Object.class)), true);
        code.cast(valueType, AnyTypeWidget.getInstance()); // , next);   // don't skip nulls
        code.exec(item.write(item.getType()));
        loop.item(code, item.read(), done, next);
        mv.visitJumpInsn(Opcodes.GOTO, next);
        mv.visitLabel(done);
    }
}
