/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.index;

import com.yahoo.yqlplus.api.types.YQLType;

public final class IndexColumn {
    private final String name;
    private final YQLType type;
    private final boolean skipEmpty;
    private final boolean skipNull;

    // Should this represent HASH or ORDERED?


    public IndexColumn(String name, YQLType type) {
        this.name = name;
        this.type = type;
        this.skipEmpty = false;
        this.skipNull = true;
    }

    public IndexColumn(String name, YQLType type, boolean skipEmpty, boolean skipNull) {
        this.name = name;
        this.type = type;
        this.skipEmpty = skipEmpty;
        this.skipNull = skipNull;
    }

    public boolean isSkipEmpty() {
        return skipEmpty;
    }

    public boolean isSkipNull() {
        return skipNull;
    }

    public String getName() {
        return name;
    }

    public YQLType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexColumn that = (IndexColumn) o;

        if (!name.equals(that.name)) return false;
        return type.equals(that.type);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
