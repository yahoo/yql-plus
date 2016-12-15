/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.IndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class ListIndexAdapter extends JavaIterableAdapter implements IndexAdapter {
    private final Class<?> clazz;
    private final TypeWidget ownerType;
    private final TypeWidget valueType;

    public ListIndexAdapter(Class<?> clazz, TypeWidget ownerType, TypeWidget valueType) {
        super(valueType);
        this.clazz = clazz;
        this.ownerType = ownerType;
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getKey() {
        return BaseTypeAdapter.INT32;
    }

    @Override
    public TypeWidget getValue() {
        return valueType;
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression) {
        return new ListAssignableValue(getValue(), target, indexExpression);
    }

    @Override
    public BytecodeExpression length(BytecodeExpression inputExpr) {
        return new CollectionSizeExpression(inputExpr);
    }
}
