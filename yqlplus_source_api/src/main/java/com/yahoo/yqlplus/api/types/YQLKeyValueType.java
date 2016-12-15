/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;

public abstract class YQLKeyValueType extends YQLValueType {
    protected final YQLType keyType;

    public YQLKeyValueType(Annotations annnotations, YQLCoreType type, String name, YQLType keyType, YQLType valueType) {
        super(annnotations, type, name, valueType);
        this.keyType = keyType;
    }

    public YQLType getKeyType() {
        return keyType;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        YQLKeyValueType that = (YQLKeyValueType) o;
        if (!keyType.equals(that.keyType)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + keyType.hashCode();
        return result;
    }

    @Override
    public void hashTo(Hasher digest) {
        super.hashTo(digest);
        valueType.hashTo(digest);
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        if (!super.internalAssignableFrom(source)) {
            return false;
        }
        YQLKeyValueType other = (YQLKeyValueType) source;
        return keyType.isAssignableFrom(other.keyType);
    }
}
