/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;


import java.lang.reflect.Type;

public interface TypeAdaptingWidget {
    boolean supports(Class<?> clazzType);

    TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type);
}
