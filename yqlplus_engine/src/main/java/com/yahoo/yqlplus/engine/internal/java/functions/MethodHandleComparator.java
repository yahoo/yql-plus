/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import java.lang.invoke.MethodHandle;
import java.util.Comparator;

public final class MethodHandleComparator<T> implements Comparator<T> {
    private final MethodHandle handle;

    public MethodHandleComparator(MethodHandle handle) {
        this.handle = handle;
    }


    @Override
    public int compare(T o1, T o2) {
        try {
            return (int) handle.invoke(o1, o2);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
