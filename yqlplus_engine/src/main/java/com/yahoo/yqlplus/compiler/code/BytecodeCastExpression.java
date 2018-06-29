/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

public class BytecodeCastExpression extends BaseTypeExpression {
    private BytecodeExpression value;

    public BytecodeCastExpression(TypeWidget targetType, BytecodeExpression value) {
        super(targetType);
        this.value = value;
    }

    @Override
    public void generate(CodeEmitter code) {
        value.generate(code);
        code.cast(getType(), value.getType());
    }
}
