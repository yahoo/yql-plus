/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;


public final class YQLMapType extends YQLKeyValueType {

    YQLMapType(YQLType keyType, YQLType valueType) {
        this(Annotations.EMPTY, keyType, valueType);
    }

    YQLMapType(Annotations annotations, YQLType keyType, YQLType valueType) {
        super(annotations, YQLCoreType.MAP, "map<" + keyType.getName() + "," + valueType.getName() + ">", keyType, valueType);
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLMapType(newAnnotations, keyType, valueType);
    }

    public static boolean is(YQLType type) {
        return type instanceof YQLMapType;
    }

    public static YQLMapType create(YQLType key, YQLType value) {
        return new YQLMapType(key, value);
    }

}
