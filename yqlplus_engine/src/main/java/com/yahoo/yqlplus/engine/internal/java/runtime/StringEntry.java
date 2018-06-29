/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.runtime;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.yahoo.yqlplus.api.trace.TraceLogEntry;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

final class StringEntry implements TraceLogEntry, JsonSerializable {
    private final int id;
    private final Level level;
    private final long ticks;
    private final String message;

    public StringEntry(int id, String message, long ticks, Level level) {
        this.id = id;
        this.message = message;
        this.ticks = ticks;
        this.level = level;
    }

    public StringEntry(int id, long ticks, Level level, String message, Object... formatArgs) {
        this.id = id;
        if (formatArgs != null && formatArgs.length > 0) {
            this.message = String.format(message, formatArgs);
        } else {
            this.message = message;
        }
        this.ticks = ticks;
        this.level = level;
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
        return message;
    }

    @Override
    public String toString() {
        return message;
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
        // TODO: we'd like an internal-view which gave copious details as well as an external-view which only did this
        jgen.writeStartObject();
        jgen.writeStringField("level", level.getName());
        jgen.writeNumberField("t", TimeUnit.NANOSECONDS.toMicros(ticks));
        jgen.writeStringField("message", message);
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
        serialize(jgen, provider);
    }

}
