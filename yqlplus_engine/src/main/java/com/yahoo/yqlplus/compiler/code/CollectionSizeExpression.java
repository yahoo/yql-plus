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

import java.util.Collection;

public class CollectionSizeExpression extends BaseTypeExpression {
    private final BytecodeExpression target;

    public CollectionSizeExpression(BytecodeExpression target) {
        super(BaseTypeAdapter.INT32);
        this.target = target;
    }

    @Override
    public void generate(CodeEmitter code) {
        Label done = new Label();
        Label isNull = new Label();
        MethodVisitor mv = code.getMethodVisitor();
        code.exec(target);
        boolean nullable = code.nullTest(target.getType(), isNull);
        code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Collection.class), "size", Type.getMethodDescriptor(Type.INT_TYPE), true);
        if (nullable) {
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(isNull);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitLabel(done);
        }
    }
}
