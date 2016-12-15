/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.runtime;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public final class PropagateFuture<V> implements FutureCallback<V> {
    public static <V> void propagate(ListenableFuture<? extends V> source, SettableFuture<V> target) {
        Futures.addCallback(source, new PropagateFuture<V>(target));
    }

    private final SettableFuture<V> target;

    protected PropagateFuture(SettableFuture<V> target) {
        this.target = target;
    }

    @Override
    public void onSuccess(V result) {
        this.target.set(result);
    }

    @Override
    public void onFailure(Throwable t) {
        this.target.setException(t);
    }
}
