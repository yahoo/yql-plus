/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.network.api;

import com.yahoo.yqlplus.api.types.YQLType;

import java.lang.reflect.Type;
import java.util.Map;

public interface ProgramDescriptor {
    public interface ArgumentInfo {
        YQLType getType();

        boolean isRequired();
    }

    String getName();

    Map<String, ArgumentInfo> getArgumentDescriptor();

    Map<String, YQLType> getResultDescriptor();

    // how will each result type be represented?
    Map<String, Type> getJvmResultTypes();
}
