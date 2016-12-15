/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.internal.bytecode.IterableTypeWidget;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

class DynamicPropertyAdapter extends OpenPropertyAdapter {
    public DynamicPropertyAdapter() {
        super(AnyTypeWidget.getInstance());
    }

    @Override
    public AssignableValue property(final BytecodeExpression target, final String propertyName) {
        return new AssignableValue() {
            @Override
            public TypeWidget getType() {
                return AnyTypeWidget.getInstance();
            }

            @Override
            public BytecodeExpression read() {
                return AnyTypeWidget.invokeDynamic("dyn:getProp|getElem:" + propertyName, AnyTypeWidget.getInstance(), target, ImmutableList.<BytecodeExpression>of());
            }

            @Override
            public BytecodeSequence write(BytecodeExpression value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeSequence write(TypeWidget top) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void generate(CodeEmitter code) {
                code.exec(read());
            }
        };
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression propertyName) {
        return new DynamicPropertyAssignableValue(target, propertyName);
    }

    @Override
    public BytecodeExpression getPropertyNameIterable(BytecodeExpression target) {
        return AnyTypeWidget.invokeDynamic("yql:getFieldNames",
                new IterableTypeWidget(BaseTypeAdapter.STRING), target, ImmutableList.<BytecodeExpression>of());
    }
}
