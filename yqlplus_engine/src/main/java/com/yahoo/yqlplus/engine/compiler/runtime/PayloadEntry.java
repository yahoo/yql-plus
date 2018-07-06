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
import com.yahoo.yqlplus.api.trace.TraceLogEntry;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

class PayloadEntry implements TraceLogEntry, JsonSerializable {
    private final int id;
    private final long ticks;
    private final Level level;
    private final Object payload;

    PayloadEntry(int id, long ticks, Level level, Object payload) {
        this.id = id;
        this.ticks = ticks;
        this.level = level;
        this.payload = payload;
    }

    @Override
    public int getTraceId() {
        return id;
    }

    @Override
    public Level getLevel() {
        return level;
    }

    @Override
    public long getTicks() {
        return ticks;
    }

    @Override
    public Object get() {
        return payload;
    }

    @Override
    public String toString() {
        return payload.toString();
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
        // TODO: we'd like an internal-view which gave copious details as well as an external-view which only did this
        jgen.writeStartObject();
        jgen.writeStringField("level", level.getName());
        jgen.writeNumberField("t", TimeUnit.NANOSECONDS.toMicros(ticks));
        provider.defaultSerializeField("message", payload, jgen);
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
        serialize(jgen, provider);
    }
}
