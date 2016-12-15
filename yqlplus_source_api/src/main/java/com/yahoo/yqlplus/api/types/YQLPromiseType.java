/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public final class YQLPromiseType extends YQLValueType {
    public YQLPromiseType(Annotations annnotations, YQLType valueType) {
        super(annnotations, YQLCoreType.PROMISE, "promise<" + valueType.getName() + ">", valueType);
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLPromiseType(newAnnotations, valueType);
    }
}
