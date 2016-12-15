/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler.streams;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public interface StreamElement {
    TypeWidget getOutputRowType();

    Writer createWriter();

    public interface Writer extends IterateAdapter.IterateLoop {
        void prepare(BytecodeExpression ctxExpr, BytecodeExpression stream, CodeEmitter code);

        void setNext(Writer out);

        void begin(CodeEmitter code);

        void end(CodeEmitter code);

        void iterate(BytecodeExpression input, CodeEmitter code);
    }
}
