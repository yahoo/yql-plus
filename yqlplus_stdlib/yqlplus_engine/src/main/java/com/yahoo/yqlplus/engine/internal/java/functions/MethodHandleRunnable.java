/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import java.lang.invoke.MethodHandle;

public final class MethodHandleRunnable implements Runnable {
    private final MethodHandle handle;

    public MethodHandleRunnable(MethodHandle handle) {
        this.handle = handle;
    }

    @Override
    public void run() {
        try {
            handle.invoke();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
