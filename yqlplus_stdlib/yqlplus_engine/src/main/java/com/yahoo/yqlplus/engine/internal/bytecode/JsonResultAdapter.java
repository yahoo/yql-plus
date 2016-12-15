/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ResultAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.TodoException;

public class JsonResultAdapter implements SerializationAdapter {
    private final TypeWidget typeWidget;
    private final ResultAdapter resultAdapter;

    public JsonResultAdapter(TypeWidget typeWidget, ResultAdapter resultAdapter) {
        this.typeWidget = typeWidget;
        this.resultAdapter = resultAdapter;
    }

    @Override
    public BytecodeSequence serializeTo(BytecodeExpression source, BytecodeExpression generator) {
        throw new TodoException();
    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        throw new TodoException();
    }
}
