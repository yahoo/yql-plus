/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public final class YQLErrorType extends YQLNamedValueType {
    public static final YQLType STACKFRAME = YQLStructType.builder()
            .addField("line", YQLBaseType.INT32, true)
            .addField("offset", YQLBaseType.INT32, true)
            .addField("name", YQLBaseType.STRING)
            .build();
    public static final YQLType STACKTRACE = YQLStructType.builder()
            .addField("message", YQLBaseType.STRING)
            .addField("frames", YQLArrayType.create(STACKFRAME), true)
            .build();

    public static final YQLType ERROR = new YQLErrorType();

    private YQLErrorType() {
        super(Annotations.EMPTY, YQLCoreType.ERROR, "error",
                YQLStructType.builder()
                        .addField("code", YQLBaseType.INT32, true)
                        .addField("type", YQLBaseType.STRING, true)
                        .addField("message", YQLBaseType.STRING)
                        .addField("details", YQLArrayType.create(STACKTRACE), true)
                        .build());
    }

    private YQLErrorType(Annotations annnotations, YQLCoreType type, String name, YQLType valueType) {
        super(annnotations, type, name, valueType);
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLErrorType(newAnnotations, getCoreType(), getName(), valueType);
    }
}
