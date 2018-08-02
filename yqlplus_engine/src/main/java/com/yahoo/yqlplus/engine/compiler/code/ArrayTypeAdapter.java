/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import java.lang.reflect.Type;

public class ArrayTypeAdapter implements TypeAdaptingWidget {
    @Override
    public boolean supports(Class<?> clazzType) {
        return clazzType.isArray();
    }

    @Override
    public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
        Class<?> arrayType = JVMTypes.getRawType(type);
        Class<?> componentType = arrayType.getComponentType();
        TypeWidget componentAdapted = typeAdapter.adapt(componentType);
        return new ArrayTypeWidget(arrayType, componentAdapted);
    }
}
