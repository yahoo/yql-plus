/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.runtime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.engine.api.Record;

import java.util.*;

public abstract class KeyAccumulator<KEY extends Comparable, RECORD extends Record> implements Iterable<RECORD> {
    public static KeyAccumulator concat(List<KeyAccumulator> cursors) {
        final LinkedHashSet<Object> out = Sets.newLinkedHashSet();
        for (KeyAccumulator cursor : cursors) {
            for (Object key : cursor) {
                out.add(key);
            }
        }
        return new KeyAccumulator() {
            @Override
            protected Record createKey(List columns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator iterator() {
                return out.iterator();
            }
        };
    }

    private final Map<String, Integer> names = Maps.newTreeMap();
    private final List<String> keyNames = Lists.newArrayList();
    private final List<Set<KEY>> seen = Lists.newArrayList();
    private final List<List<KEY>> columns = Lists.newArrayList();
    // not very clever, but this can be optimized
    private List<RECORD> output;

    public KeyAccumulator(Object... instructions) {
        if (instructions != null) {
            init(instructions);
        }
    }

    public KeyAccumulator() {
    }

    private void init(Object[] instructions) {
        for (int i = 0; i < instructions.length; i += 2) {
            String name = (String) instructions[i];
            List<KEY> keys = (List<KEY>) instructions[i + 1];
            addKey(name, keys);
        }
        generate();
    }

    private int addKey(String name, List<KEY> keys) {
        Set<KEY> seen;
        List<KEY> output;
        int idx;
        if (names.containsKey(name)) {
            idx = names.get(name);
            keyNames.add(name);
            seen = this.seen.get(idx);
            output = columns.get(idx);
        } else {
            idx = columns.size();
            keyNames.add(name);
            names.put(name, idx);
            output = Lists.newArrayListWithExpectedSize(keys.size());
            columns.add(output);
            seen = Sets.newHashSet();
            this.seen.add(seen);
        }
        // preserve order but uniqueify
        for (KEY key : keys) {
            if (seen.add(key)) {
                output.add(key);
            }
        }
        return idx;
    }

    private void generate() {
        int count = 1;
        for (List<KEY> key : columns) {
            count *= key.size();
        }
        if (count == 0) {
            output = ImmutableList.of();
            return;
        }
        output = Lists.newArrayListWithCapacity(count);
        List<KEY> row = Lists.newArrayListWithCapacity(columns.size());
        generate(0, row);
    }

    protected abstract RECORD createKey(List<KEY> columns);

    private void generate(int i, List<KEY> row) {
        if (columns.size() == row.size()) {
            output.add(createKey(row));
        } else {
            for (KEY key : columns.get(i)) {
                row.add(key);
                generate(i + 1, row);
                row.remove(i);
            }
        }
    }

    @Override
    public Iterator<RECORD> iterator() {
        return Iterators.unmodifiableIterator(output.iterator());
    }

    public int size() {
        return output.size();
    }
}
