/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public final class YQLOptionalType extends YQLValueType {
    public static boolean is(YQLType target) {
        return target instanceof YQLOptionalType;
    }

    public static YQLType deoptional(YQLType target) {
        if (target instanceof YQLOptionalType) {
            return ((YQLOptionalType) target).getValueType();
        }
        return target;
    }

    public static YQLType create(YQLType output) {
        return is(output) ? output : new YQLOptionalType(Annotations.EMPTY, output);
    }


    YQLOptionalType(Annotations annotations, YQLType valueType) {
        super(annotations, YQLCoreType.OPTIONAL, "optional<" + deoptional(valueType).getName() + ">", deoptional(valueType));
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLOptionalType(newAnnotations, valueType);
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        return super.isAssignableFrom(source) || valueType.isAssignableFrom(source);
    }

}
