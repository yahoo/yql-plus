/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.sequences;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.api.Record;

import java.io.IOException;
import java.util.List;

public final class Sequences {
    private Sequences() {

    }

    public static final class EmptyRecord implements Record, JsonSerializable {
        private EmptyRecord() {
        }

        @Override
        public Iterable<String> getFieldNames() {
            return ImmutableList.of();
        }

        @Override
        public Object get(String field) {
            throw new IllegalArgumentException("Field '" + field + "' not found");
        }

        @Override
        public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeEndObject();
        }

        @Override
        public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
            jgen.writeStartObject();
            jgen.writeEndObject();
        }
    }

    private static final Record EMPTY_RECORD = new EmptyRecord();

    public static List<Record> singletonSequence() {
        return ImmutableList.of(EMPTY_RECORD);
    }


}
