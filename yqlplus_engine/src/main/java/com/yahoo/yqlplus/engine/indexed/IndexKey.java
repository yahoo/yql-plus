/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.indexed;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class IndexKey {
    final List<String> columnOrder;
    final List<String> columns;
    final List<String> lowerCaseColumns;

    private IndexKey(Iterable<String> cols) {
        this.columnOrder = ImmutableList.copyOf(cols);
        List<String> copy = Lists.newArrayList(cols);
        Collections.sort(copy);
        this.columns = copy;
        lowerCaseColumns = new ArrayList<>(this.columns.size());
        for (String col:this.columns) {
            lowerCaseColumns.add(col.toLowerCase());
        }
        Collections.sort(lowerCaseColumns);
    }

    public static IndexKey of(Iterable<String> cols) {
        return new IndexKey(cols);
    }

    public static IndexKey of(String... cols) {
        return new IndexKey(Arrays.asList(cols));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexKey indexKey = (IndexKey) o;

        if (lowerCaseColumns.size() != indexKey.lowerCaseColumns.size()) return false;
        for (int i = 0; i < lowerCaseColumns.size(); i++) {
            if (!lowerCaseColumns.get(i).equals(indexKey.lowerCaseColumns.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return lowerCaseColumns.hashCode();
    }
}
