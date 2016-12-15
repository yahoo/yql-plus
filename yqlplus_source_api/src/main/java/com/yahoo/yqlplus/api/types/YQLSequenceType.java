/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public final class YQLSequenceType extends YQLValueType {

    public static YQLType create(YQLType elementType) {
        return new YQLSequenceType(elementType);
    }

    YQLSequenceType(YQLType elementType) {
        this(Annotations.EMPTY, elementType);
    }

    YQLSequenceType(Annotations annotations, YQLType elementType) {
        super(annotations, YQLCoreType.SEQUENCE, "sequence<" + elementType.getName() + ">", elementType);
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLSequenceType(newAnnotations, valueType);
    }

    public static boolean is(YQLType type) {
        return type instanceof YQLSequenceType;
    }

}
