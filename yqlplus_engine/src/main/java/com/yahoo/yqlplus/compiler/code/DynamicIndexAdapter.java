/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.ImmutableList;

public class DynamicIndexAdapter implements IndexAdapter {

    @Override
    public TypeWidget getKey() {
        return AnyTypeWidget.getInstance();
    }

    @Override
    public TypeWidget getValue() {
        return AnyTypeWidget.getInstance();
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression) {
        return new DynamicPropertyAssignableValue(target, indexExpression);
    }

    @Override
    public BytecodeExpression length(BytecodeExpression inputExpr) {
        return AnyTypeWidget.invokeDynamic("dyn:getLength", BaseTypeAdapter.INT32, inputExpr, ImmutableList.of());
    }
}
