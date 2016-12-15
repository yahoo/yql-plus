/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

import java.util.List;

public interface CallableInvocable extends GambitCreator.Invocable {
    @Override
    CallableInvocable prefix(BytecodeExpression... arguments);

    @Override
    CallableInvocable prefix(List<BytecodeExpression> arguments);

    TypeWidget getResultType();
}
