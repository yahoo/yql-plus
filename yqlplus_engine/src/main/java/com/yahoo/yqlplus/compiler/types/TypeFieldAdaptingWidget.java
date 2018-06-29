/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;

import com.yahoo.yqlplus.compiler.code.EngineValueTypeAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;

public class TypeFieldAdaptingWidget implements TypeAdaptingWidget {
    @Override
    public boolean supports(Class<?> clazzType) {
        try {
            Field fld = clazzType.getField("$TYPE$");
            if (Modifier.isPublic(fld.getModifiers()) && Modifier.isStatic(fld.getModifiers()) && TypeWidget.class.isAssignableFrom(fld.getType())) {
                return true;
            }
        } catch (NoSuchFieldException ignored) {
        }
        return false;
    }

    @Override
    public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
        final Class<?> clazzType = JVMTypes.getRawType(type);
        try {
            Field fld = clazzType.getField("$TYPE$");
            if (Modifier.isStatic(fld.getModifiers()) && TypeWidget.class.isAssignableFrom(fld.getType())) {
                return (TypeWidget) clazzType.getField("$TYPE$").get(null);
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
        }
        return null;
    }
}
