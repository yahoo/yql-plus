/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordAccumulator<OUTPUT> {
    private final ConcurrentLinkedQueue<Object> rows = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean done = new AtomicBoolean(false);

    public List complete() {
        if (done.compareAndSet(false, true)) {
            return finish(Lists.newArrayList(rows));
        } else {
            throw new IllegalStateException();
        }
    }

    protected List<OUTPUT> finish(List candidate) {
        return candidate;
    }

    public boolean receive(Object row) {
        rows.add(row);
        return true;
    }

    public boolean receiveAll(List<Object> row) {
        rows.addAll(row);
        return true;
    }
}
