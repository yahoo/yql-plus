/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class JoinTask implements Runnable {
    private final AtomicInteger count;

    protected JoinTask(int count) {
        this.count = new AtomicInteger(count);
    }

    public abstract void exec();

    public final void run() {
        if (count.decrementAndGet() == 0) {
            exec();
        }
    }
}
