/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;

public abstract class LValue {
    public abstract BytecodeExpression read();

    public abstract BytecodeSequence write(final BytecodeExpression value);

    public abstract BytecodeSequence write();
}
