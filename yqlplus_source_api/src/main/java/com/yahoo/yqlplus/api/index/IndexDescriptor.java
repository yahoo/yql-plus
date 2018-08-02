/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.index;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.annotations.Experimental;
import com.yahoo.yqlplus.api.types.YQLBaseType;
import com.yahoo.yqlplus.api.types.YQLCoreType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

@Experimental
public final class IndexDescriptor {
    private static final EnumSet<YQLCoreType> INDEX_TYPES = EnumSet.of(
            YQLCoreType.INT32,       // int or Integer
            YQLCoreType.INT64,       // long or Long
            YQLCoreType.TIMESTAMP,   // long or Long
            YQLCoreType.BYTES,       // ByteBuffer or byte[]
            YQLCoreType.STRING,      // String
            YQLCoreType.FLOAT32,     // float or Float
            YQLCoreType.FLOAT64);    // double or Double

    public static class Builder {
        private List<IndexColumn> columns = Lists.newArrayList();
        private List<String> columnNames = Lists.newArrayList();

        private Builder() {
        }

        public Builder addColumn(String name, YQLCoreType type) {
            return addColumn(name, type, false, true);
        }

        public Builder addColumn(String name, YQLCoreType type, boolean skipEmpty, boolean skipNull) {
            if (columnNames.contains(name)) {
                throw new IllegalArgumentException("Index already has column '" + name + "'");
            }
            if (!INDEX_TYPES.contains(type)) {
                throw new IllegalArgumentException("Index column types must be one of INT32, INT64, TIMESTAMP, BYTES, STRING, FLOAT32, or FLOAT64");
            }
            columnNames.add(name);
            columns.add(new IndexColumn(name, YQLBaseType.get(type), skipEmpty, skipNull));
            return this;
        }

        public IndexDescriptor build() {
            return new IndexDescriptor(columns, columnNames);
        }

    }

    public static Builder builder() {
        return new Builder();
    }

    private final String name;
    private final List<IndexColumn> columns;
    private final List<String> columnNames;
    private final IndexName key;

    private IndexDescriptor(List<IndexColumn> columns, List<String> columnNames) {
        this.columns = Collections.unmodifiableList(columns);
        this.columnNames = Collections.unmodifiableList(columnNames);
        this.key = IndexName.of(columnNames);
        StringBuilder nameBuilder = new StringBuilder();
        for (int i = 0; i < columnNames.size(); ++i) {
            if (i > 0) {
                nameBuilder.append(",");
            }
            nameBuilder.append(columnNames.get(i));
        }
        this.name = nameBuilder.toString();
    }

    public List<IndexColumn> getColumns() {
        return columns;
    }

    public IndexColumn getColumn(String name) {
        for (IndexColumn col : columns) {
            if (name.equalsIgnoreCase(col.getName())) {
                return col;
            }
        }
        return null;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public IndexName getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexDescriptor that = (IndexDescriptor) o;

        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

}