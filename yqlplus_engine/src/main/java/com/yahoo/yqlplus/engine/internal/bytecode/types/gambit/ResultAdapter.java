/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;


public interface ResultAdapter {
    TypeWidget getResultType();

    BytecodeExpression createSuccess(BytecodeExpression input);

    BytecodeExpression createFailureThrowable(BytecodeExpression input);

    BytecodeExpression createFailureYQLError(BytecodeExpression input);

    BytecodeExpression resolve(BytecodeExpression target);

    BytecodeExpression isSuccess(BytecodeExpression target);
}
