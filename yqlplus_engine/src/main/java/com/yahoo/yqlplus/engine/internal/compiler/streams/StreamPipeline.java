/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler.streams;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;

public interface StreamPipeline {
    BytecodeExpression create(BytecodeExpression ctxExpr);

    BytecodeSequence feed(BytecodeExpression ctxExpr, BytecodeExpression stream, BytecodeExpression iterable);

    BytecodeSequence materialize(BytecodeExpression ctxExpr, BytecodeExpression stream);
}
