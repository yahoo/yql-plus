/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.scope;

import com.google.inject.Key;

public final class WrapScope extends MapExecutionScope {
    private ExecutionScope child;

    public WrapScope(ExecutionScope child) {
        this.child = child;
    }

    @Override
    public <T> T get(Key<T> key) {
        T result = super.get(key);
        if (result != null) {
            return result;
        } else {
            return child.get(key);
        }
    }
}
