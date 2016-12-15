/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.types;

import com.google.common.base.Joiner;
import com.google.inject.TypeLiteral;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

final class Types {
    @SuppressWarnings("unchecked")
    static <T> Class<T> get(Type type) {
        if (type instanceof Class<?>) {
            return (Class<T>) type;
        }
        if (type instanceof ParameterizedType) {
            return (Class<T>) ((ParameterizedType) type).getRawType();
        }

        throw new ClassCastException(String.format("Fail to resolve type %s to Class", type));
    }

    static Type getType(Class<?> clz, Type type, TypeLiteral<?> typeLiteral) {
        return (clz == type) ? clz : typeLiteral.getType();
    }

    static ParameterizedType createParameterizedType(Type rawType, Type... args) {
        return new ParameterizedTypeImpl(rawType, args);
    }

    private Types() {
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
            builder.append("(" + super.toString() + "): ");
            builder.append(rawType);
            builder.append("<");
            Joiner j = Joiner.on(",");
            builder.append(j.join(args));
            builder.append(">");
            return builder.toString();
        }
    }

}
