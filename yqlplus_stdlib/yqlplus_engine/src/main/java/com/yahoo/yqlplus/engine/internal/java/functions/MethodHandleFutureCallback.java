/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import com.google.common.util.concurrent.FutureCallback;

import java.lang.invoke.MethodHandle;

public final class MethodHandleFutureCallback<V> implements FutureCallback<V> {
    private final MethodHandle success;
    private final MethodHandle failure;

    public MethodHandleFutureCallback(MethodHandle success, MethodHandle failure) {
        this.success = success;
        this.failure = failure;
    }

    @Override
    public void onSuccess(V v) {
        try {
            success.invoke(v);
        } catch (Error | RuntimeException error) {
            throw error;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public void onFailure(Throwable throwable) {
        try {
            failure.invoke(throwable);
        } catch (Error | RuntimeException error) {
            throw error;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
