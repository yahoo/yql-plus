/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.yahoo.yqlplus.engine.CompiledProgram;

import java.lang.reflect.Type;

public class CompiledResultSetInfo implements CompiledProgram.ResultSetInfo {
    private final String name;
    private final Type type;

    public CompiledResultSetInfo(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getResultType() {
        return type;
    }
}
