/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types;

import com.google.common.base.Joiner;
import com.google.inject.TypeLiteral;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class JVMTypes {
    public static Type getTypeArgument(Type type, int i) {
        if (type instanceof ParameterizedType) {
            Type[] typeArgument = ((ParameterizedType) type).getActualTypeArguments();
            if (typeArgument.length > i - 1) {
                return typeArgument[i];
            }
        }
        return Object.class;
    }

    public static Class<?> getRawType(Type type) {
        return TypeLiteral.get(type).getRawType();
    }

    public static ParameterizedType createParameterizedType(Type rawType, Type... args) {
        return new ParameterizedTypeImpl(rawType, args);
    }


    private static final class ParameterizedTypeImpl implements ParameterizedType {

        private final Type rawType;
        private final Type[] args;

        public ParameterizedTypeImpl(Type rawType, Type... args) {
            this.rawType = rawType;
            this.args = args;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return args;
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            builder.append(super.toString());
            builder.append("): ");
            builder.append(rawType);
            builder.append("<");
            Joiner j = Joiner.on(",");
            builder.append(j.join(args));
            builder.append(">");
            return builder.toString();
        }
    }


}
