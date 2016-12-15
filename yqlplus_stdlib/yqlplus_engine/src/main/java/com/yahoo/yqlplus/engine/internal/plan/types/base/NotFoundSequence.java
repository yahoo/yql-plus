/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;

public class NotFoundSequence implements BytecodeSequence {
    private final BytecodeExpression target;
    private final BytecodeExpression propertyName;

    public NotFoundSequence(BytecodeExpression target, BytecodeExpression propertyName) {
        this.target = target;
        this.propertyName = propertyName;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.emitThrow(PropertyNotFoundException.class, propertyName);
    }
}
