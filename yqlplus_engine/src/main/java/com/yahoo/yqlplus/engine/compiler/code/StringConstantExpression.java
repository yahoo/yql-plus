/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public class StringConstantExpression extends BaseTypeExpression implements EvaluatedExpression {
    private String value;

    public StringConstantExpression(String value) {
        super(BaseTypeAdapter.STRING);
        this.value = value;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.emitStringConstant(value);
    }
}
