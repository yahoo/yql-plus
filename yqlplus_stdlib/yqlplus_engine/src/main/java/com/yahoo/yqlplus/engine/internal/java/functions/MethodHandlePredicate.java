/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import com.google.common.base.Predicate;

import java.lang.invoke.MethodHandle;

public final class MethodHandlePredicate<T> implements Predicate<T> {
    private final MethodHandle handle;

    public MethodHandlePredicate(MethodHandle handle) {
        this.handle = handle;
    }

    @Override
    public boolean apply(T t) {
        try {
            return (boolean) handle.invoke(t);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

}
