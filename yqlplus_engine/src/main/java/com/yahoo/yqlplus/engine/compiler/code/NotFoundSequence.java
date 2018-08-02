/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;

public class NotFoundSequence implements BytecodeSequence {
    private final BytecodeExpression propertyName;

    public NotFoundSequence(BytecodeExpression propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.emitThrow(PropertyNotFoundException.class, propertyName);
    }
}
