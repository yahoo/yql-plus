/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.yahoo.yqlplus.api.trace.Timeout;

import java.util.concurrent.Callable;

public abstract class ForkableTask implements Callable<Object> {
    public abstract Timeout getTimeout();

    @Override
    public final Object call() throws Exception {
        return invoke();
    }

    protected abstract Object invoke() throws Exception;
}
