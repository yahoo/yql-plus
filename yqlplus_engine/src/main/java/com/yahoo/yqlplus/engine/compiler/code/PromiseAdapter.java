/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public interface PromiseAdapter {
    TypeWidget getResultType();

    BytecodeExpression resolve(ScopedBuilder scope, BytecodeExpression timeout, BytecodeExpression target);
}

