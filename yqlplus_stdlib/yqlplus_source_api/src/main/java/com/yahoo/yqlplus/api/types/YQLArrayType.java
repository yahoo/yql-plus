/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public final class YQLArrayType extends YQLValueType {

    public static YQLType create(YQLType elementType) {
        return new YQLArrayType(elementType);
    }

    YQLArrayType(YQLType elementType) {
        this(Annotations.EMPTY, elementType);
    }

    YQLArrayType(Annotations annotations, YQLType elementType) {
        super(annotations, YQLCoreType.ARRAY, "array<" + elementType.getName() + ">", elementType);
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLArrayType(newAnnotations, valueType);
    }

    public static boolean is(YQLType type) {
        return type instanceof YQLArrayType;
    }

}
