/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;

public abstract class YQLNamedValueType extends YQLValueType {
    public YQLNamedValueType(Annotations annnotations, YQLCoreType type, String name, YQLType valueType) {
        super(annnotations, type, name, valueType);
    }

    public boolean isRequired() {
        return !(getValueType() instanceof YQLOptionalType);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
                + (31 * getName().hashCode());
    }

    public void hashTo(Hasher digest) {
        super.hashTo(digest);
        digest.putUnencodedChars(getName());
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        return super.internalAssignableFrom(source) && getName().equals(source.getName());
    }
}
