/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.scope;

import com.google.inject.Key;

public class EmptyExecutionScope implements ExecutionScope {
    @Override
    public <T> T get(Key<T> key) {
        return null;
    }
}
