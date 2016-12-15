/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class JacksonValueSerializationAdapter implements SerializationAdapter {
    private final TypeWidget sourceType;

    public JacksonValueSerializationAdapter(TypeWidget sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public BytecodeSequence serializeTo(BytecodeExpression source, BytecodeExpression generator) {
        if (sourceType.isPrimitive()) {
            return new JacksonSerializePrimitive(source, generator);
        }
        return new JacksonSerializeObject(source, generator);
    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        throw new UnsupportedOperationException();
    }
}
