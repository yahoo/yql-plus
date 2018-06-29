/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.inject.TypeLiteral;

import java.lang.reflect.Type;

public class ReflectiveTypeAdapter implements TypeAdaptingWidget {
    @Override
    public boolean supports(Class<?> clazzType) {
        return true;
    }

    @Override
    public TypeWidget adapt(EngineValueTypeAdapter asmProgramTypeAdapter, Type type) {
        return new ReflectiveJavaTypeWidget(asmProgramTypeAdapter, TypeLiteral.get(type));
    }
}
