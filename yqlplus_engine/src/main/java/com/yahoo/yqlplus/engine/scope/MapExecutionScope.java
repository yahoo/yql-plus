/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.scope;

import com.google.common.collect.Maps;
import com.google.inject.Key;
import com.google.inject.name.Names;

import java.lang.reflect.Type;
import java.util.Map;

public class MapExecutionScope implements ExecutionScope {
    private Map<Key<?>, Object> bindings = Maps.newHashMap();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Key<T> key) {
        return (T) bindings.get(key);
    }

    // the general case
    public MapExecutionScope bind(Key<?> key, Object binding) {
        bindings.put(key, binding);
        return this;
    }

    // convenience for a couple common cases, but the rest can use the general case
    public MapExecutionScope bind(Type type, Object binding) {
        return bind(Key.get(type), binding);
    }

    public MapExecutionScope bind(Type type, String name, Object binding) {
        return bind(Key.get(type, Names.named(name)), binding);
    }
}
