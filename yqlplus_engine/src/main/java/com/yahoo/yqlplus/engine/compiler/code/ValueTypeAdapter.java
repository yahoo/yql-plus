/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.inject.TypeLiteral;

import java.lang.reflect.Type;

public interface ValueTypeAdapter {

    TypeWidget adaptInternal(TypeLiteral<?> typeLiteral);

    TypeWidget adaptInternal(Type type);

    TypeWidget adaptInternal(Type type, boolean nullable);

    TypeWidget adaptInternal(Class<?> clazz);

    TypeWidget adaptInternal(Class<?> clazz, boolean nullable);
}
