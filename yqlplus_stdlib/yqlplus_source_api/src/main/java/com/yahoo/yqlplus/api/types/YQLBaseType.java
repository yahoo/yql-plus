/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;

import java.util.EnumSet;

public final class YQLBaseType extends YQLType implements Comparable<YQLBaseType> {
    private YQLBaseType(YQLCoreType type) {
        super(Annotations.EMPTY, type, type.name().toLowerCase());
    }

    private YQLBaseType(Annotations annotations, YQLCoreType type) {
        super(annotations, type, type.name().toLowerCase());
    }


    public static final YQLType VOID = new YQLBaseType(YQLCoreType.VOID);

    public static final YQLType ANY = new YQLBaseType(YQLCoreType.ANY);
    public static final YQLBaseType INT8 = new YQLBaseType(YQLCoreType.INT8);
    public static final YQLBaseType INT16 = new YQLBaseType(YQLCoreType.INT16);
    public static final YQLBaseType INT32 = new YQLBaseType(YQLCoreType.INT32);
    public static final YQLBaseType INT64 = new YQLBaseType(YQLCoreType.INT64);
    public static final YQLBaseType FLOAT32 = new YQLBaseType(YQLCoreType.FLOAT32);
    public static final YQLBaseType FLOAT64 = new YQLBaseType(YQLCoreType.FLOAT64);
    public static final YQLBaseType STRING = new YQLBaseType(YQLCoreType.STRING);
    public static final YQLBaseType BYTES = new YQLBaseType(YQLCoreType.BYTES);
    public static final YQLBaseType TIMESTAMP = new YQLBaseType(YQLCoreType.TIMESTAMP);
    public static final YQLBaseType BOOLEAN = new YQLBaseType(YQLCoreType.BOOLEAN);

    public static EnumSet<YQLCoreType> INTEGERS = EnumSet.of(YQLCoreType.INT8, YQLCoreType.INT16, YQLCoreType.INT32, YQLCoreType.INT64);
    public static EnumSet<YQLCoreType> FLOATS = EnumSet.of(YQLCoreType.FLOAT32, YQLCoreType.FLOAT64);


    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLBaseType(newAnnotations, getCoreType());
    }

    public boolean equals(Object other) {
        return (other instanceof YQLBaseType) && ((YQLBaseType) other).getCoreType() == this.getCoreType();
    }

    @Override
    public int hashCode() {
        return getCoreType().hashCode();
    }

    @Override
    public void hashTo(Hasher digest) {
        digest.putUnencodedChars(YQLBaseType.class.getName());
        digest.putUnencodedChars(getCoreType().name());
    }

    @Override
    public int compareTo(YQLBaseType o) {
        return Integer.compare(getCoreType().ordinal(), o.getCoreType().ordinal());
    }


    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        if (source.getCoreType() == getCoreType()) {
            return true;
        }
        if (INTEGERS.contains(getCoreType()) && INTEGERS.contains(source.getCoreType()) || (FLOATS.contains(getCoreType()) && FLOATS.contains(source.getCoreType()))) {
            return getCoreType().ordinal() < source.getCoreType().ordinal();
        }
        return false;
    }

    public static YQLType get(YQLCoreType type) {
        switch (type) {
            case VOID:
                return VOID;
            case BOOLEAN:
                return BOOLEAN;
            case INT8:
                return INT8;
            case INT16:
                return INT16;
            case INT32:
                return INT32;
            case INT64:
                return INT64;
            case TIMESTAMP:
                return TIMESTAMP;
            case FLOAT32:
                return FLOAT32;
            case FLOAT64:
                return FLOAT64;
            case STRING:
                return STRING;
            case BYTES:
                return BYTES;
        }
        throw new IllegalArgumentException(type + " is not a base core type");
    }
}
