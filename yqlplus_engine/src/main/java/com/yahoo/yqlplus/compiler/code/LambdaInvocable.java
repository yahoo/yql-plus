/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import java.util.List;

public interface LambdaInvocable extends GambitCreator.Invocable {
    @Override
    LambdaInvocable prefix(BytecodeExpression... arguments);

    @Override
    LambdaInvocable prefix(List<BytecodeExpression> arguments);

    TypeWidget getResultType();
}
