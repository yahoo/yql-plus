/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Annotations implements Iterable<Map.Entry<String, Object>> {
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        public Builder put(String key, int value) {
            builder.put(key, value);
            return this;
        }

        public Builder put(String key, String value) {
            builder.put(key, value);
            return this;
        }

        public Builder with(Object... kvPairs) {
            updateBuilder(builder, kvPairs);
            return this;
        }

        public Annotations build() {
            return new Annotations(builder.build());
        }
    }

    public static final Annotations EMPTY = new Annotations(ImmutableMap.<String, Object>of()) {
        @Override
        public final boolean hasAnnotation(String name) {
            return false;
        }

        @Override
        protected final Object getAnnotation(String name) {
            return null;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return Collections.emptyIterator();
        }
    };

    private final Annotations parent;
    private final Map<String, Object> annotations;

    private Annotations(Map<String, Object> annotations) {
        this.parent = EMPTY;
        this.annotations = ImmutableMap.copyOf(annotations);
    }

    private Annotations(Annotations parent, Map<String, Object> annotations) {
        this.parent = parent;
        this.annotations = annotations;
    }

    public boolean hasAnnotation(String name) {
        return annotations.containsKey(name) || parent.hasAnnotation(name);
    }

    protected Object getAnnotation(String name) {
        if (annotations.containsKey(name)) {
            return annotations.get(name);
        } else {
            return parent.getAnnotation(name);
        }
    }

    public int getIntAnnotation(String name) {
        return (Integer) annotations.get(name);
    }

    public String getStringAnnotation(String name) {
        return (String) annotations.get(name);
    }

    private static Object checkType(Object input) {
        Preconditions.checkNotNull(input, "Annotation value must not be null");
        Preconditions.checkArgument(input instanceof String || input instanceof Integer, "Annotation value must be String or int");
        return input;
    }

    public Annotations withAnnotations(Object... kvPairs) {
        return new Annotations(this, updateBuilder(ImmutableMap.<String, Object>builder(), kvPairs).build());
    }

    private static ImmutableMap.Builder<String, Object> updateBuilder(ImmutableMap.Builder<String, Object> builder, Object[] kvPairs) {
        for (int i = 0; i < kvPairs.length; i += 2) {
            String key = (String) kvPairs[i];
            Object value = checkType(kvPairs[i + 1]);
            builder.put(key, value);
        }
        return builder;
    }

    public void hashTo(Hasher digest) {
        for (Map.Entry<String, Object> e : annotations.entrySet()) {
            digest.putUnencodedChars(e.getKey());
            // we're going to assume value is a String or Number because the creation code enforces that
            digest.putUnencodedChars(e.getValue().toString());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Annotations that = (Annotations) o;

        if (!annotations.equals(that.annotations)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return annotations.hashCode();
    }

    public Annotations merge(Annotations newAnnotations) {
        if (annotations.isEmpty()) {
            return newAnnotations;
        } else {
            Map<String, Object> a = Maps.newLinkedHashMap();
            a.putAll(annotations);
            a.putAll(newAnnotations.annotations);
            return new Annotations(a);
        }
    }

    @Override
    public Iterator<Map.Entry<String, Object>> iterator() {
        // we don't expect much use of this
        final Set<String> seen = Sets.newTreeSet();
        return Iterators.filter(Iterators.concat(annotations.entrySet().iterator(), parent.iterator()),
                new Predicate<Map.Entry<String, Object>>() {
                    @Override
                    public boolean apply(Map.Entry<String, Object> input) {
                        return seen.add(input.getKey());
                    }
                }
        );
    }
}
