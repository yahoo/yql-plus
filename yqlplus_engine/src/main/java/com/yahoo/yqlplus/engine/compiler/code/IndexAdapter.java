/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public interface IndexAdapter {
    TypeWidget getKey();

    TypeWidget getValue();

    AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression);

    BytecodeExpression length(BytecodeExpression inputExpr);
}
