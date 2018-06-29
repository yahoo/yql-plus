/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

public final class InstructionConstant extends BaseTypeExpression implements EvaluatedExpression {
    private final int op;

    public InstructionConstant(TypeWidget type, int op) {
        super(type);
        this.op = op;
    }

    @Override
    public void generate(CodeEmitter environment) {
        environment.getMethodVisitor().visitInsn(op);
    }
}
