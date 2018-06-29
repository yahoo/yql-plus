/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

public class DynamicPropertyAssignableValue implements AssignableValue {
    private final BytecodeExpression target;
    private final BytecodeExpression indexExpression;

    public DynamicPropertyAssignableValue(BytecodeExpression target, BytecodeExpression indexExpression) {
        this.target = target;
        this.indexExpression = indexExpression;
    }

    @Override
    public TypeWidget getType() {
        return AnyTypeWidget.getInstance();
    }

    @Override
    public BytecodeExpression read() {
        return AnyTypeWidget.invokeDynamic("dyn:getElem|getProp", AnyTypeWidget.getInstance(), target, ImmutableList.of(indexExpression));
    }

    @Override
    public BytecodeSequence write(BytecodeExpression value) {
        return AnyTypeWidget.invokeDynamic("dyn:setElem|setProp", BaseTypeAdapter.VOID, target, ImmutableList.of(indexExpression, value));
    }

    @Override
    public BytecodeSequence write(final TypeWidget type) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                AssignableValue local = code.allocate(type);
                local.write(type).generate(code);
                AnyTypeWidget.invokeDynamic("dyn:setElem|setProp",
                        BaseTypeAdapter.VOID,
                        target,
                        ImmutableList.of(indexExpression, local.read())).generate(code);
            }
        };
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }

}
