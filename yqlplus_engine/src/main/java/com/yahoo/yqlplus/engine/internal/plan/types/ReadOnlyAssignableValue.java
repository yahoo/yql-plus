/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;

public abstract class ReadOnlyAssignableValue implements AssignableValue {
    private final TypeWidget type;

    public ReadOnlyAssignableValue(TypeWidget type) {
        this.type = type;
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    @Override
    public abstract BytecodeExpression read();

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
        read().generate(code);
    }
}
