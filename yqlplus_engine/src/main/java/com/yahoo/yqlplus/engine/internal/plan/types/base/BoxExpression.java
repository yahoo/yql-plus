/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class BoxExpression implements BytecodeExpression {
    private final TypeWidget boxed;
    private final BytecodeExpression input;

    public BoxExpression(BytecodeExpression input) {
        this.boxed = input.getType().boxed();
        this.input = input;
    }

    @Override
    public TypeWidget getType() {
        return boxed;
    }

    @Override
    public void generate(CodeEmitter code) {
        input.generate(code);
        code.box(input.getType());
    }
}
