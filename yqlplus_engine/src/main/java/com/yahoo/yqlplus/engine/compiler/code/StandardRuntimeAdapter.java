/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public class StandardRuntimeAdapter implements RuntimeAdapter {
    private TypeWidget widget;
    public StandardRuntimeAdapter(TypeWidget widget) {
        this.widget = widget;
    }

    @Override
    public BytecodeExpression property(BytecodeExpression target, BytecodeExpression propertyName) {
        if(widget.hasProperties()) {
            return widget.getPropertyAdapter().index(target, propertyName);
        } else {
            return new NullExpr(AnyTypeWidget.getInstance());
        }
    }

    @Override
    public BytecodeExpression property(BytecodeExpression target, BytecodeExpression propertyName, BytecodeExpression defaultValue) {
        if(widget.hasProperties()) {
            return widget.getPropertyAdapter().index(target, propertyName, defaultValue);
        } else {
            return new NullExpr(AnyTypeWidget.getInstance());
        }
    }


    @Override
    public BytecodeExpression index(BytecodeExpression target, BytecodeExpression indexExpr) {
        if(widget.isIndexable()) {
            return widget.getIndexAdapter().index(target, indexExpr);
        } else {
            return new NullExpr(AnyTypeWidget.getInstance());
        }
    }

    @Override
    public BytecodeSequence mergeIntoFieldWriter(BytecodeExpression source, BytecodeExpression fieldWriter) {
        if(widget.hasProperties()) {
            return widget.getPropertyAdapter().mergeIntoFieldWriter(source, fieldWriter);
        }
        return BytecodeSequence.NOOP;
    }
}
