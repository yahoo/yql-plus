/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;

import com.fasterxml.jackson.core.JsonGenerator;
import com.yahoo.yqlplus.compiler.runtime.FieldWriter;

public abstract class RuntimeWidget {
    public Object propertyObject(Object source, Object propertyName) {
        if (propertyName instanceof String) {
            return property(source, (String) propertyName);
        }
        return null;
    }

    public abstract Object property(Object source, String propertyName);

    public abstract Object index(Object source, Object index);

    public abstract void serializeJson(Object source, JsonGenerator generator);

    public abstract void mergeIntoFieldWriter(Object source, FieldWriter writer);

    public abstract Iterable<String> getFieldNames(Object source);
}
