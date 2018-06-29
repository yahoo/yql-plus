/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.yahoo.yqlplus.engine.api.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public abstract class StructBase implements Record, JsonSerializable, Map<Object, Object> {
    private final Predicate<String> IS_SET = new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String input) {
            return get(input) != null;
        }
    };

    @Override
    public final Iterable<String> getFieldNames() {
        return Iterables.filter(getAllFieldNames(), IS_SET);
    }

    public abstract Iterable<String> getAllFieldNames();

    public void mergeFields(FieldWriter writer) {
        for (String fieldName : getAllFieldNames()) {
            Object value = get(fieldName);
            if (value != null) {
                writer.put(fieldName, value);
            }
        }
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        for (String fieldName : getAllFieldNames()) {
            Object value = get(fieldName);
            if (value != null) {
                provider.defaultSerializeField(fieldName, value, jgen);
            }
        }
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
        // we just don't support this
        serialize(jgen, provider);
    }

    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other.getClass() != this.getClass()) {
            return false;
        }
        Record them = (Record) other;
        for (String fieldName : getAllFieldNames()) {
            Object o = get(fieldName);
            Object t = them.get(fieldName);
            if (o == null || t == null) {
                if (!(o == null && t == null)) {
                    return false;
                }
            } else if (!t.equals(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (String fieldName : getAllFieldNames()) {
            Object o = get(fieldName);
            if (o != null) {
                result = 31 * result + o.hashCode();
            }
        }
        return result;
    }

    @Override
    public int size() {
        return Iterables.size(getFieldNames());
    }

    @Override
    public boolean isEmpty() {
        return size() > 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return Iterables.contains(getFieldNames(), key);
    }

    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            return false;
        }
        for (String field : getAllFieldNames()) {
            Object candidate = get(field);
            if (value.equals(candidate)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object get(Object key) {
        return get((String) key);
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends Object, ?> m) {
        for (Map.Entry<? extends Object, ?> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Object> keySet() {
        return ImmutableSet.copyOf(getFieldNames());
    }

    @Override
    public Collection<Object> values() {
        return ImmutableList.copyOf(new ValueIterable());
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
        return new AbstractSet<Entry<Object, Object>>() {
            @Override
            public Iterator<Entry<Object, Object>> iterator() {
                return new EntryIterable().iterator();
            }

            @Override
            public int size() {
                return StructBase.this.size();
            }
        };
    }

    @Override
    public Object put(Object key, Object value) {
        Object v = get((String) key);
        set((String) key, value);
        return v;
    }

    protected abstract void set(String key, Object value);

    private class ValueIterable implements Iterable<Object> {
        @Override
        public Iterator<Object> iterator() {
            final Iterator<String> fields = getAllFieldNames().iterator();
            return new AbstractIterator<Object>() {
                @Override
                protected Object computeNext() {
                    while (fields.hasNext()) {
                        String field = fields.next();
                        Object value = get(field);
                        if (value != null) {
                            return value;
                        }
                    }
                    return endOfData();
                }
            };
        }
    }

    private class EntryIterable implements Iterable<Entry<Object, Object>> {
        @Override
        public Iterator<Entry<Object, Object>> iterator() {
            final Iterator<String> fields = getAllFieldNames().iterator();
            return new AbstractIterator<Entry<Object, Object>>() {
                @Override
                protected Entry<Object, Object> computeNext() {
                    while (fields.hasNext()) {
                        String field = fields.next();
                        Object value = get(field);
                        if (value != null) {
                            return Maps.immutableEntry(field, value);
                        }
                    }
                    return endOfData();
                }
            };
        }
    }
}
