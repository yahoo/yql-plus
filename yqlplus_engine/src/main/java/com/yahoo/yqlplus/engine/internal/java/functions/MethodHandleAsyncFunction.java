/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import java.lang.invoke.MethodHandle;
import java.util.List;

public final class MethodHandleAsyncFunction<F,T> implements AsyncFunction<F,T> {
    private final MethodHandle handle;

    public MethodHandleAsyncFunction(MethodHandle handle) {
        this.handle = handle;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListenableFuture<T> apply(F t) {
        try {
            if (handle.type().parameterCount() > 1 && t instanceof List) {
                return (ListenableFuture<T>) handle.invokeWithArguments((List) t);
            } else {
                return (ListenableFuture<T>) handle.invoke(t);
            }
        } catch (Error | RuntimeException error) {
            throw error;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

}
