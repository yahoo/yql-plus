/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public interface AssignableValue extends BytecodeExpression {
    TypeWidget getType();

    BytecodeExpression read();

    BytecodeSequence write(BytecodeExpression value);

    // write top of stack
    BytecodeSequence write(TypeWidget top);
}
