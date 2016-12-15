/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompiledArgumentInfo implements CompiledProgram.ArgumentInfo {
    private final String name;
    private final Type type;
    private final boolean required;
    private final YQLType yqlType;
    private final Object defaultValue;

    public CompiledArgumentInfo(String name, Type type, Object defaultValue) {
        this.name = name;
        this.type = type;
        this.required = (null == defaultValue);
        this.yqlType = null;
        this.defaultValue = defaultValue;
    }

    public CompiledArgumentInfo(String name, YQLType type, Object defaultValue) {
        this.name = name;
        this.type = convertYQLCoreType(type);
        this.required = (null == defaultValue);
        this.yqlType = type;
        this.defaultValue = defaultValue;
    }

    public static Type convertYQLCoreType(YQLType yqlType) {
        Type resultType;
        switch (yqlType.getCoreType()) {
        case INT8:
            resultType = byte.class;
            break;
        case INT16:
            resultType = short.class;
            break;
        case INT32:
            resultType = int.class;
            break;
        case INT64:
            resultType = long.class;
            break;
        case FLOAT32:
            resultType = float.class;
            break;
        case FLOAT64:
            resultType = double.class;
            break;
        case STRING:
            resultType = String.class;
            break;
        case BYTES:
            resultType = ByteBuffer.class;
            break;
        case TIMESTAMP:
            resultType = long.class;
            break;
        case BOOLEAN:
            resultType = boolean.class;
            break;
        case MAP:
            resultType = Map.class;
            break;
        case ARRAY:
            resultType = Object[].class;
            break;
        case STRUCT:
            resultType = Record.class;
            break;
        default:
            throw new ProgramCompileException("Unsupported argtype: " + yqlType);
        }
        return resultType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public boolean isRequired() {
        return required;
    }
    
    @Override
    public YQLType getYQLType() {
        return yqlType;
    }
    
    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }
}
