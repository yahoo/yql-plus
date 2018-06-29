/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.exprs;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.types.NotNullableTypeWidget;

public class NullTestedExpression implements BytecodeExpression {
    private TypeWidget type;
    private BytecodeExpression input;

    public NullTestedExpression(BytecodeExpression input) {
        this.input = input;
        this.type = NotNullableTypeWidget.create(input.getType());
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    @Override
    public void generate(CodeEmitter code) {
        input.generate(code);
    }
}
