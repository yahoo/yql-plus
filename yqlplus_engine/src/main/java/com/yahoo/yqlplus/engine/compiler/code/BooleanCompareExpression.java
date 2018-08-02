/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.engine.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class BooleanCompareExpression extends BaseTypeExpression {
    private final Location loc;
    private final BytecodeExpression leftExpr;
    private final BytecodeExpression rightExpr;
    private final BinaryComparison booleanComparison;

    public BooleanCompareExpression(Location loc, BytecodeExpression leftExpr, BytecodeExpression rightExpr, BinaryComparison booleanComparison) {
        super(BaseTypeAdapter.BOOLEAN);
        this.loc = loc;
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
        this.booleanComparison = booleanComparison;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(new CompareExpression(loc, leftExpr, rightExpr));
        MethodVisitor mv = code.getMethodVisitor();
        Label isTrue = new Label();
        Label done = new Label();
        switch (booleanComparison) {
            case LT:
                mv.visitJumpInsn(Opcodes.IFLT, isTrue);
                break;
            case LTEQ:
                mv.visitJumpInsn(Opcodes.IFLE, isTrue);
                break;
            case GT:
                mv.visitJumpInsn(Opcodes.IFGT, isTrue);
                break;
            case GTEQ:
                mv.visitJumpInsn(Opcodes.IFGE, isTrue);
                break;
        }
        code.emitBooleanConstant(false);
        mv.visitJumpInsn(Opcodes.GOTO, done);
        mv.visitLabel(isTrue);
        code.emitBooleanConstant(true);
        mv.visitLabel(done);
    }
}
