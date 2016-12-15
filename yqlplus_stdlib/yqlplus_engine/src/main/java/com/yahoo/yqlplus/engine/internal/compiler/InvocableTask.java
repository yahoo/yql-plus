/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;


import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitCreator;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;

public interface InvocableTask {
    BytecodeExpression createRunnable(GambitCreator.ScopeBuilder scopeBuilder);
}
