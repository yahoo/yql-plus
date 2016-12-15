/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;


import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;

public interface BytecodeExpression extends BytecodeSequence {
    TypeWidget getType();

    void generate(CodeEmitter code);
}
