/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;

public abstract class YQLType {
    private final Annotations annotations;
    private final YQLCoreType coreType;
    private final String name;

    YQLType(Annotations annnotations, YQLCoreType type, String name) {
        this.annotations = annnotations;
        this.coreType = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public YQLCoreType getCoreType() {
        return coreType;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof YQLType)) {
            return false;
        }
        YQLType that = (YQLType) other;
        return that.getCoreType() == getCoreType();
    }

    @Override
    public int hashCode() {
        return getCoreType().hashCode() +
                (31 * annotations.hashCode());
    }

    public void hashTo(Hasher digest) {
        digest.putInt(getCoreType().ordinal());
        annotations.hashTo(digest);
    }

    public abstract YQLType withAnnotations(Annotations newAnnotations);

    public final YQLType withAnnotationMerge(Object... kvPairs) {
        return withAnnotations(annotations.withAnnotations(kvPairs));
    }

    public final Annotations getAnnotations() {
        return annotations;
    }

    public String toString() {
        return name;
    }

    public boolean isAssignableFrom(YQLType source) {
        if (source.getCoreType() == YQLCoreType.ANY) {
            return true;
        }
        return internalAssignableFrom(YQLOptionalType.deoptional(source));
    }

    protected abstract boolean internalAssignableFrom(YQLType source);
}
