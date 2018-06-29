/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.exprs;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class EqualsExpression implements BytecodeExpression {
    private final Location loc;
    private BytecodeExpression leftExpr;
    private BytecodeExpression rightExpr;
    private final boolean negate;

    public EqualsExpression(Location loc, BytecodeExpression leftExpr, BytecodeExpression rightExpr, boolean negate) {
        this.loc = loc;
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
        this.negate = negate;
    }

    @Override
    public TypeWidget getType() {
        return BaseTypeAdapter.BOOLEAN;
    }

    @Override
    public void generate(CodeEmitter code) {
        // a bit of a hack; should not need to go to dynamic invocation for this unless one arg is ANY
        MethodVisitor mv = code.getMethodVisitor();
        Label hasNull = new Label();
        CodeEmitter.Unification unified = code.unifiedEmit(leftExpr, rightExpr, hasNull);
        if (unified.type.isPrimitive()) {
            emitPrimitiveEquals(code, unified.type);
        } else {
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(Object.class), "equals", Type.getMethodDescriptor(Type.BOOLEAN_TYPE,
                    Type.getType(Object.class)), false);
            if (negate) {
                emitNegate(mv);
            }
        }
        if (unified.nullPossible) {
            Label done = new Label();
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(hasNull);
            emitFalse(code);
            mv.visitLabel(done);
        }
    }

    private void emitPrimitiveEquals(CodeEmitter code, TypeWidget type) {
        switch (type.getJVMType().getSort()) {
            case Type.BYTE:
            case Type.BOOLEAN:
            case Type.SHORT:
            case Type.INT:
            case Type.CHAR:
                emitIntEquals(code);
                break;
            case Type.FLOAT:
                emitComplexEquals(code, Opcodes.FCMPG);
                break;
            case Type.LONG:
                emitComplexEquals(code, Opcodes.LCMP);
                break;
            case Type.DOUBLE:
                emitComplexEquals(code, Opcodes.DCMPG);
                break;
            default:
                throw new UnsupportedOperationException("Unexpected primitive type: " + leftExpr.getType().getJVMType().getDescriptor());
        }
    }

    private void emitComplexEquals(CodeEmitter code, int op) {
        MethodVisitor mv = code.getMethodVisitor();
        mv.visitInsn(op);
        Label done = new Label();
        Label isTrue = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, isTrue);
        emitFalse(code);
        mv.visitJumpInsn(Opcodes.GOTO, done);
        mv.visitLabel(isTrue);
        emitTrue(code);
        mv.visitLabel(done);
    }

    private void emitIntEquals(CodeEmitter code) {
        Label done = new Label();
        MethodVisitor mv = code.getMethodVisitor();
        Label isTrue = new Label();
        mv.visitJumpInsn(Opcodes.IF_ICMPEQ, isTrue);
        emitFalse(code);
        mv.visitJumpInsn(Opcodes.GOTO, done);
        mv.visitLabel(isTrue);
        emitTrue(code);
        mv.visitLabel(done);
    }

    private void emitFalse(CodeEmitter code) {
        code.getMethodVisitor().visitInsn(negate ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
    }

    private void emitTrue(CodeEmitter code) {
        code.getMethodVisitor().visitInsn(negate ? Opcodes.ICONST_0 : Opcodes.ICONST_1);
    }

    private void emitNegate(MethodVisitor mv) {
        Label truth = new Label();
        Label skip = new Label();
        mv.visitJumpInsn(Opcodes.IFNE, truth);
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitJumpInsn(Opcodes.GOTO, skip);
        mv.visitLabel(truth);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitLabel(skip);
    }
}
