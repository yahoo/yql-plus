/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class IndexName {
    private final List<String> columns;

    private IndexName(List<String> columns) {
        this.columns = columns;
    }

    public static IndexName of(List<String> cols) {
        List<String> copy = Lists.newArrayList(cols);
        Collections.sort(copy);
        return new IndexName(Collections.unmodifiableList(copy));
    }

    public static IndexName of(String... cols) {
        Preconditions.checkNotNull(cols);
        return of(Arrays.asList(cols));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexName indexName = (IndexName) o;

        return columns.equals(indexName.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        boolean first = true;
        for (String name : columns) {
            if (!first) {
                out.append(",");
            }
            first = false;
            out.append(name);
        }
        return out.toString();
    }
}
