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

public class StructIndexAdapter implements IndexAdapter {
    private final TypeWidget owner;
    private final PropertyAdapter propertyAdapter;

    public StructIndexAdapter(TypeWidget owner, PropertyAdapter propertyAdapter) {
        this.owner = owner;
        this.propertyAdapter = propertyAdapter;
    }

    @Override
    public TypeWidget getKey() {
        return BaseTypeAdapter.STRING;
    }

    @Override
    public TypeWidget getValue() {
        return AnyTypeWidget.getInstance();
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression) {
        return new CastAssignableValue(getValue(), propertyAdapter.index(target, indexExpression));
    }

    @Override
    public BytecodeExpression length(BytecodeExpression inputExpr) {
        throw new UnsupportedOperationException();
    }
}
