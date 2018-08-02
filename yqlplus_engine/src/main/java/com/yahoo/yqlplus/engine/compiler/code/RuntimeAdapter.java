/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public interface RuntimeAdapter {
    // Object property(Object target, String propertyName);
    BytecodeExpression property(BytecodeExpression target, BytecodeExpression propertyName);

    BytecodeExpression property(BytecodeExpression target, BytecodeExpression propertyName, BytecodeExpression defaultValue);

    // Object property(Object target, Object indexExpr);
    BytecodeExpression index(BytecodeExpression target, BytecodeExpression indexExpr);

    // void mergeIntoFieldWriter(Object target, FieldWriter target);
    BytecodeSequence mergeIntoFieldWriter(BytecodeExpression source, BytecodeExpression map);
}
