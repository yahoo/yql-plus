/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.internal.bytecode.types.JVMTypes;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;

public abstract class ToyMemoryTableSource<T> implements Source {
    static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();

    protected void load(InputStream data, final Class<T> recordType) throws IOException {
        JsonParser parser = JSON_FACTORY.createParser(data);
        List<T> records = parser.readValueAs(new TypeReference<List<T>>() {
            @Override
            public Type getType() {
                return JVMTypes.createParameterizedType(List.class, recordType);
            }
        });
        for (T record : records) {
            ingest(record);
        }
    }

    protected abstract void ingest(T record);
}
