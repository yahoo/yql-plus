/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.inject.TypeLiteral;

import java.lang.reflect.Type;

public interface ValueTypeAdapter{

    TypeWidget adapt(TypeLiteral<?> typeLiteral);

    TypeWidget adapt(TypeLiteral<?> typeLiteral, boolean nullable);

    TypeWidget adapt(Type type);

    TypeWidget adapt(Type type, boolean nullable);

    TypeWidget adapt(Class<?> clazz);

    TypeWidget adapt(Class<?> clazz, boolean nullable);
}
