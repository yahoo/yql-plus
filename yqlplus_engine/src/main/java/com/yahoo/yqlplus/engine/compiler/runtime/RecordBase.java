/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.yahoo.yqlplus.engine.api.Record;

import java.io.IOException;

public abstract class RecordBase implements Record, JsonSerializable {
    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        for (String fieldName : getFieldNames()) {
            Object value = get(fieldName);
            if (value != null) {
                provider.defaultSerializeField(fieldName, value, jgen);
            }
        }
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
        // we just don't support this
        serialize(jgen, provider);
    }
}
