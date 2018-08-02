/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public class JavaIterableAdapter implements IterateAdapter {
    private final TypeWidget valueType;

    public JavaIterableAdapter(TypeWidget valueType) {
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getValue() {
        return valueType;
    }

    @Override
    public BytecodeSequence iterate(final BytecodeExpression target, final IterateLoop loop) {
        return new IterateSequence(target, valueType, loop);
    }

    @Override
    public BytecodeSequence iterate(BytecodeExpression target, AssignableValue item, IterateLoop loop) {
        return new IterateSequence(target, valueType, item, loop);
    }

    @Override
    public BytecodeExpression first(BytecodeExpression target) {
        return new IterateFirstSequence(target, valueType);
    }
}
