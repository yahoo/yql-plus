/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MethodDispatcher {
    private final TypeLiteral<?> target;
    private Map<String, TypeLiteral<?>> matches = Maps.newHashMap();

    public MethodDispatcher(TypeLiteral<?> target) {
        this.target = target;
    }

    private String toKey(String methodName, List<TypeWidget> argumentTypes) {
        StringBuilder out = new StringBuilder();
        out.append(methodName)
                .append('(');
        Joiner.on(",").appendTo(out, Iterables.transform(argumentTypes, new Function<TypeWidget, String>() {
            @Override
            public String apply(TypeWidget input) {
                return input.getJVMType().getDescriptor();
            }
        }));
        out.append(')');
        return out.toString();
    }


    private String toKey(String methodName, Iterable<Type> argumentTypes) {
        StringBuilder out = new StringBuilder();
        out.append(methodName)
                .append('(');
        Joiner.on(",").appendTo(out, Iterables.transform(argumentTypes, new Function<Type, String>() {
            @Override
            public String apply(Type input) {
                return input.getDescriptor();
            }
        }));
        out.append(')');
        return out.toString();
    }

    static class MethodMatch implements Comparable<MethodMatch> {
        TypeLiteral<?> returnType;
        Method method;
        int score;

        MethodMatch(TypeLiteral<?> returnType, Method method, int score) {
            this.returnType = returnType;
            this.method = method;
            this.score = score;
        }

        @Override
        public int compareTo(MethodMatch o) {
            return Integer.compare(this.score, o.score);
        }
    }


    // for now, we only worry about argument count, leaving coercion of type to runtime
    public TypeLiteral<?> lookup(String methodName, Type[] argumentTypes) {
        String key = toKey(methodName, Arrays.asList(argumentTypes));
        if (matches.containsKey(key)) {
            return matches.get(key);
        }
        List<MethodMatch> candidate = Lists.newArrayList();
        Class<?> clazz = target.getRawType();
        for (Method method : clazz.getMethods()) {
            if (!Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers())) {
                if (methodName.equals(method.getName())) {
                    int count = target.getParameterTypes(method).size();
                    if (count == argumentTypes.length) {
                        candidate.add(new MethodMatch(target.getReturnType(method), method, 0));
                    } else if (method.isVarArgs() && count >= (argumentTypes.length - 1)) {
                        candidate.add(new MethodMatch(target.getReturnType(method), method, 1));
                    }
                }
            }
        }
        Collections.sort(candidate);
        if (candidate.size() == 0) {
            return null;
        } else {
            TypeLiteral<?> returnType = candidate.get(0).returnType;
            matches.put(key, returnType);
            return returnType;
        }
    }
}
