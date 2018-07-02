/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.Callable;

public interface CallableInvocableBuilder extends InvocableBuilder {
    MethodHandle getFactory() throws Throwable;

    @Override
    CallableInvocable complete(BytecodeExpression result);
}
