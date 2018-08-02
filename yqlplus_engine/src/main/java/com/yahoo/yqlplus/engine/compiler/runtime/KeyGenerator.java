/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.engine.api.Record;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class KeyGenerator<KEY extends Comparable, RECORD extends Record> implements Iterable<RECORD> {
    public interface Creator<RECORD, KEY> {
        RECORD createKey(List<KEY> columns);
    }

    public static <KEY extends Comparable, RECORD extends Record> List<RECORD> generate(Creator<RECORD, KEY> creator, Object[] instructions) {
        final Map<String, Integer> names = Maps.newTreeMap();
        final List<Set<KEY>> seen = Lists.newArrayList();
        final List<List<KEY>> columns = Lists.newArrayList();
        for (int i = 0; i < instructions.length; i += 2) {
            String name = (String) instructions[i];
            List<KEY> keys = (List<KEY>) instructions[i + 1];
            Set<KEY> keySeen;
            List<KEY> keyOutput;
            int idx;
            if (names.containsKey(name)) {
                idx = names.get(name);
                keySeen = seen.get(idx);
                keyOutput = columns.get(idx);
            } else {
                idx = columns.size();
                names.put(name, idx);
                keyOutput = Lists.newArrayListWithExpectedSize(keys.size());
                columns.add(keyOutput);
                keySeen = Sets.newHashSet();
                seen.add(keySeen);
            }
            // preserve order but uniqueify
            for (KEY key : keys) {
                if (keySeen.add(key)) {
                    keyOutput.add(key);
                }
            }
        }
        int count = 1;
        for (List<KEY> key : columns) {
            count *= key.size();
        }
        if (count == 0) {
            return ImmutableList.of();
        }
        final List<RECORD> output = Lists.newArrayListWithCapacity(count);
        List<KEY> row = Lists.newArrayListWithCapacity(columns.size());
        generate(columns, output, creator, 0, row);
        return Collections.unmodifiableList(output);
    }

    private static <KEY extends Comparable, RECORD extends Record> void generate(List<List<KEY>> columns, List<RECORD> output, Creator<RECORD, KEY> creator, int i, List<KEY> row) {
        if (columns.size() == row.size()) {
            output.add(creator.createKey(row));
        } else {
            for (KEY key : columns.get(i)) {
                row.add(key);
                generate(columns, output, creator, i + 1, row);
                row.remove(i);
            }
        }
    }
}
