/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

public class PopExpression implements BytecodeSequence {
    private final BytecodeExpression expr;

    public PopExpression(BytecodeExpression expr) {
        this.expr = expr;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(expr);
        code.pop(expr.getType());
    }
}
