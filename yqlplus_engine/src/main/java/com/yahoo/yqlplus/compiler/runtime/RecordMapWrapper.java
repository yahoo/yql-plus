/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.runtime;

import com.google.common.collect.Maps;
import com.yahoo.yqlplus.engine.api.Record;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class RecordMapWrapper implements Map<String,Object>, Record {
    private final Map<String, Object> fields;

    public RecordMapWrapper(Map<String, Object> fields) {
        this.fields = fields;
    }

    public RecordMapWrapper() {
        this.fields = Maps.newLinkedHashMap();
    }

    @Override
    public int size() {
        return fields.size();
    }

    @Override
    public boolean isEmpty() {
        return fields.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return fields.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return fields.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return fields.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        return fields.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return fields.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        fields.putAll(m);
    }

    @Override
    public void clear() {
        fields.clear();
    }

    @Override
    public Set<String> keySet() {
        return fields.keySet();
    }

    @Override
    public Collection<Object> values() {
        return fields.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return fields.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return fields.equals(o);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        return fields.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super Object> action) {
        fields.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
        fields.replaceAll(function);
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        return fields.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return fields.remove(key, value);
    }

    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        return fields.replace(key, oldValue, newValue);
    }

    @Override
    public Object replace(String key, Object value) {
        return fields.replace(key, value);
    }

    @Override
    public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
        return fields.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        return fields.computeIfPresent(key, remappingFunction);
    }

    @Override
    public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        return fields.compute(key, remappingFunction);
    }

    @Override
    public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        return fields.merge(key, value, remappingFunction);
    }

    @Override
    public Iterable<String> getFieldNames() {
        return fields.keySet();
    }

    @Override
    public Object get(String field) {
        return fields.get(field);
    }
}
