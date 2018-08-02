/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public abstract class BaseTypeExpression implements BytecodeExpression {
    private final TypeWidget type;

    public BaseTypeExpression(TypeWidget type) {
        this.type = type;
    }

    @Override
    public TypeWidget getType() {
        return type;
    }
}
