/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;

public class DynamicSerializationAdapter implements SerializationAdapter {
    private String serializeOperation;
    private String deserializeOperation;

    public DynamicSerializationAdapter(String serializeOperation, String deserializeOperation) {
        this.serializeOperation = serializeOperation;
        this.deserializeOperation = deserializeOperation;
    }

    @Override
    public BytecodeSequence serializeTo(BytecodeExpression source, BytecodeExpression generator) {
        return AnyTypeWidget.invokeDynamic(serializeOperation, BaseTypeAdapter.VOID, source, ImmutableList.<BytecodeExpression>of(generator));
    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        return AnyTypeWidget.invokeDynamic(deserializeOperation, NullableTypeWidget.create(AnyTypeWidget.getInstance()), parser, ImmutableList.<BytecodeExpression>of());
    }
}
