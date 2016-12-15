/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Type;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class MissingRequiredFieldExpr implements BytecodeExpression {
    private final String className;
    private final String methodName;
    private final String propertyName;
    private final TypeWidget type;

    public MissingRequiredFieldExpr(String className, String methodName, String propertyName, TypeWidget type) {
        this.className = className;
        this.methodName = methodName;
        this.propertyName = propertyName;
        this.type = type;
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    @Override
    public void generate(CodeEmitter code) {
        MethodVisitor mv = code.getMethodVisitor();
        mv.visitTypeInsn(Opcodes.NEW, Type.getInternalName(IllegalArgumentException.class));
        mv.visitInsn(Opcodes.DUP);
        mv.visitLdcInsn(String.format("%s::%s Missing required property '%s' (%s)", className, methodName, propertyName, type.getTypeName()));
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(IllegalArgumentException.class), "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(String.class)), false);
        mv.visitInsn(Opcodes.ATHROW);
    }
}
