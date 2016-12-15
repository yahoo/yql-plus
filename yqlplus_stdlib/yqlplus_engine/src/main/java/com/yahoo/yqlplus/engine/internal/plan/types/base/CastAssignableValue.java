/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class CastAssignableValue implements AssignableValue {
    private final TypeWidget targetType;
    private final AssignableValue value;

    public CastAssignableValue(TypeWidget targetType, AssignableValue value) {
        this.targetType = targetType;
        this.value = value;
    }

    @Override
    public TypeWidget getType() {
        return targetType;
    }

    @Override
    public BytecodeExpression read() {
        return new BytecodeCastExpression(targetType, value.read());
    }

    @Override
    public BytecodeSequence write(BytecodeExpression value) {
        return this.value.write(new BytecodeCastExpression(this.value.getType(), value));
    }

    @Override
    public BytecodeSequence write(TypeWidget top) {
        return this.value.write(top);
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }

}
