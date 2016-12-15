/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NotNullableTypeWidget;

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
