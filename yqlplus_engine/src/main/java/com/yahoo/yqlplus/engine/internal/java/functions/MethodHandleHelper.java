/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.functions;

import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public final class MethodHandleHelper {
    private MethodHandleHelper() {
    }

    public static MethodHandle uncheckedLookup(Class<?> clazz, String name, Class<?> rval, Class[] argumentTypes) {
        try {
            return MethodHandles.lookup().findVirtual(clazz, name, MethodType.methodType(rval, argumentTypes));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ProgramCompileException(e);
        }
    }

    public static MethodHandle uncheckedLookup(Class<?> clazz, String name, Class<?> rval) {
        return uncheckedLookup(clazz, name, rval, new Class<?>[0]);
    }
}
