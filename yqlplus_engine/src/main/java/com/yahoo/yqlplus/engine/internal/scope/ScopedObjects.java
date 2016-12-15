/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.google.common.collect.Maps;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;

import java.util.Map;

public class ScopedObjects {
    final ExecutionScope scope;
    private final Map<Key<?>, Object> scopedObjects = Maps.newHashMap();

    public ScopedObjects(ExecutionScope scope) {
        this.scope = scope;
    }

    synchronized <T> T get(Key<?> key, Provider<T> unscoped) {
        @SuppressWarnings("unchecked")
        T current = (T) scope.get(key);
        if (current != null) {
            return current;
        }

        @SuppressWarnings("unchecked")
        T target = (T) scopedObjects.get(key);
        if (target == null && !scopedObjects.containsKey(key)) {
            target = unscoped.get();
            scopedObjects.put(key, target);
        }
        return target;
    }

    void exit() {

    }

    void enter() {

    }

    public Map<Key<?>, Object> getScopedObjects() {
        return scopedObjects;
    }
}
