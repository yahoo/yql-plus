/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.bytecode.exprs.NullExpr;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

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
    public BytecodeExpression index(BytecodeExpression target, BytecodeExpression indexExpr) {
        if(widget.isIndexable()) {
            return widget.getIndexAdapter().index(target, indexExpr);
        } else {
            return new NullExpr(AnyTypeWidget.getInstance());
        }
    }

    @Override
    public BytecodeSequence serializeJson(BytecodeExpression source, BytecodeExpression generator) {
        return widget.getSerializationAdapter(NativeEncoding.JSON).serializeTo(source, generator);
    }

    @Override
    public BytecodeSequence serializeTBin(BytecodeExpression source, BytecodeExpression generator) {
        return widget.getSerializationAdapter(NativeEncoding.TBIN).serializeTo(source, generator);
    }

    @Override
    public BytecodeSequence mergeIntoFieldWriter(BytecodeExpression source, BytecodeExpression fieldWriter) {
        if(widget.hasProperties()) {
            return widget.getPropertyAdapter().mergeIntoFieldWriter(source, fieldWriter);
        }
        return BytecodeSequence.NOOP;
    }
}
