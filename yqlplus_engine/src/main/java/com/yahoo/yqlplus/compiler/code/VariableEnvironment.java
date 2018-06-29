/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

public interface VariableEnvironment {
    AssignableValue allocate(TypeWidget type);

    AssignableValue evaluate(BytecodeExpression expr);

    AssignableValue allocate(String name, TypeWidget type);

    AssignableValue evaluate(String name, BytecodeExpression expr);

    AssignableValue getLocal(String name);

    void alias(String from, String to);
}
