/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public final class MethodHandleCallable<T> implements Callable<T> {
    private final MethodHandle handle;

    public MethodHandleCallable(MethodHandle handle) {
        this.handle = handle;
    }

    @Override
    public T call() throws Exception {
        try {
            return (T) handle.invoke();
        } catch (Exception | Error e) {
            throw e;
        } catch (Throwable throwable) {
            throw new ExecutionException(throwable);
        }
    }
}
