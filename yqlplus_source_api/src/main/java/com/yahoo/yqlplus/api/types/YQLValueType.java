/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;

public abstract class YQLValueType extends YQLType {
    protected final YQLType valueType;

    public YQLValueType(Annotations annnotations, YQLCoreType type, String name, YQLType valueType) {
        super(annnotations, type, name);
        this.valueType = valueType;
    }

    public YQLType getValueType() {
        return valueType;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        YQLValueType that = (YQLValueType) o;

        return valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + valueType.hashCode();
        return result;
    }

    @Override
    public void hashTo(Hasher digest) {
        super.hashTo(digest);
        valueType.hashTo(digest);
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        if (source.getCoreType() == getCoreType()) {
            YQLValueType other = (YQLValueType) source;
            return valueType.isAssignableFrom(other.valueType);
        }
        return false;
    }
}
