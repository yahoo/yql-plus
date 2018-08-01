/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.source;

import com.yahoo.yqlplus.engine.compiler.runtime.FieldWriter;

import java.util.Set;

public class VerifyNoExtraFieldsFieldWriter implements FieldWriter {
    private final Set<String> fieldNames;
    private final String className;
    private final String methodName;

    public VerifyNoExtraFieldsFieldWriter(Set<String> fieldNames, String className, String methodName) {
        this.fieldNames = fieldNames;
        this.className = className;
        this.methodName = methodName;
    }

    @Override
    public void put(String fieldName, Object value) {
        if (!fieldNames.contains(fieldName)) {
            throw new IllegalArgumentException(String.format("%s::%s Unexpected additional property '%s' (%s)", className, methodName, fieldName, value.getClass().getTypeName()));
        }
    }
}
