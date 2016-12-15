/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.yahoo.yqlplus.engine.api.GeneratedSource;
import com.yahoo.yqlplus.engine.internal.code.CodeOutput;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.*;
import java.lang.reflect.Modifier;
import java.util.*;

public final class CodeFormatter {

    private static final JsonFactory JSON_FACTORY = new MappingJsonFactory();

    public static String emitLiteral(Object value) {
        if (value instanceof Enum) {
            Enum val = (Enum) value;
            return String.format("%s.%s", val.getDeclaringClass().getName(), val.name());
        } else if (value instanceof Long) {
            return String.format("%dL", (Long) value);
        } else if (value instanceof Float) {
            // the cast here silences the IDE, obviously it's being cast right back to Object in the call
            return String.format("%ff", (Float) value);
        } else if (value instanceof Number) {
            return value.toString();
        }

        if (value.getClass().isArray()) {
            // the only kind of array we support is array of object
            Object[] arr = (Object[]) value;
            if (arr.length == 0) {
                return "new Object[0]";
            } else {
                return String.format("new Object[%d] { %s }", arr.length, Joiner.on(", ").join(Iterables.transform(Arrays.asList(arr), new Function<Object, Object>() {
                    @Nullable
                    @Override
                    public Object apply(Object input) {
                        return emitLiteral(input);
                    }
                })));
            }
        }
        StringWriter out = new StringWriter();
        try {
            // we're relying on the fact that Java and JSON literals for scalar types look the same
            JsonGenerator gen = JSON_FACTORY.createGenerator(out);
            gen.writeObject(value);
            gen.flush();
            return out.toString();
        } catch (IOException e) {
            // should never happen with a stringwriter
            throw new RuntimeException(e);
        }
    }

    public static void exceptionGrovel(Throwable t) {
        grovelException(t);
        if (t.getCause() != null) {
            exceptionGrovel(t.getCause());
        }
    }

    public static void writeException(Throwable t, CodeOutput out) {
        try {
            for (StackTraceElement elt : t.getStackTrace()) {
                String cn = elt.getClassName();
                out.println("at %s", elt);
                System.err.println(elt);
                if (cn.contains(".internal.programs.Program")) {
                    Class<?> clazz = Class.forName(cn);
                    String methodName = elt.getMethodName();
                    for (Method method : clazz.getMethods()) {
                        if (method.getName().equals(methodName)) {
                            GeneratedSource source = method.getAnnotation(GeneratedSource.class);
                            if (source != null) {
                                out.indent();
                                toDumpString(Joiner.on("").join(source.values()), out);
                                out.dedent();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace(new PrintWriter(out.getWriter()));
        }
    }

    public static String toDumpString(String code) {
        StringBuilder output = new StringBuilder();
        Formatter formatter = new Formatter(output);
        StringTokenizer tok = new StringTokenizer(code, "\n");
        int line = 0;
        while (tok.hasMoreTokens()) {
            formatter.format("%3d: %s\n", ++line, tok.nextToken());
        }
        return output.toString();
    }

    public static String toDumpString(String code, CodeOutput output) {
        StringTokenizer tok = new StringTokenizer(code, "\n");
        int line = 0;
        while (tok.hasMoreTokens()) {
            output.println("%3d: %s\n", ++line, tok.nextToken());
        }
        return output.toString();
    }

    public static Iterable<String> toDumpLines(final String code) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new AbstractIterator<String>() {
                    StringTokenizer tok = new StringTokenizer(code, "\n");
                    int line = 0;

                    @Override
                    protected String computeNext() {
                        if (tok.hasMoreTokens()) {
                            return String.format("%3d: %s", ++line, tok.nextToken());
                        }
                        return endOfData();
                    }
                };
            }
        };
    }

    private static void grovelException(Throwable t) {
        try {
            System.err.println("Exception: " + t.getMessage());
            for (StackTraceElement elt : t.getStackTrace()) {
                String cn = elt.getClassName();
                System.err.println(elt);
                if (cn.contains(".internal.programs.Program")) {
                    Class<?> clazz = Class.forName(cn);
                    String methodName = elt.getMethodName();
                    for (Method method : clazz.getMethods()) {
                        if (method.getName().equals(methodName)) {
                            GeneratedSource source = method.getAnnotation(GeneratedSource.class);
                            if (source != null) {
                                System.err.println(toDumpString(Joiner.on("").join(source.values())));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String generateUniqueClassSuffix() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    // until JA 1.17 can be used
    public static class ClassTypeSignature {
        private final ClassTypeSignature declaringClass;
        private final String className;
        private final ClassTypeSignature[] typeArguments;

        private static final ClassTypeSignature[] NO_TYPE_ARGUMENTS = new ClassTypeSignature[0];

        private ClassTypeSignature(ClassTypeSignature declaringClass, String className, ClassTypeSignature[] typeArguments) {
            this.declaringClass = declaringClass;
            this.className = className;
            this.typeArguments = typeArguments;
        }

        private ClassTypeSignature(String className, ClassTypeSignature[] typeArguments) {
            this.declaringClass = null;
            this.className = className;
            this.typeArguments = typeArguments;
        }

        private ClassTypeSignature(String className) {
            this.declaringClass = null;
            this.className = className;
            this.typeArguments = NO_TYPE_ARGUMENTS;
        }

        private ClassTypeSignature(ClassTypeSignature declaringClass, String className) {
            this.declaringClass = declaringClass;
            this.className = className;
            this.typeArguments = NO_TYPE_ARGUMENTS;
        }

        public String toString() {
            return encode();
        }

        public String encode() {
            StringBuilder out = new StringBuilder();
            encode(out);
            return out.toString();
        }

        void encodeName(StringBuilder out) {
            if (declaringClass != null) {
                declaringClass.encodeName(out);
                out.append('$');
            }
            out.append(className.replace('.', '/'));
        }

        public void encode(StringBuilder out) {
            out.append('L');
            encodeName(out);
            if (typeArguments.length > 0) {
                out.append('<');
                for (ClassTypeSignature arg : typeArguments) {
                    arg.encode(out);
                }
                out.append('>');
            }
            out.append(';');
        }
    }

    static ClassTypeSignature[] toTypeArguments(Type[] types) {
        ClassTypeSignature[] out = new ClassTypeSignature[types.length];
        for (int i = 0; i < types.length; ++i) {
            out[i] = toObjectType(types[i]);
        }
        return out;
    }

    static <T> Class<T> get(Type type) {
        if (type instanceof Class<?>) {
            return (Class<T>) type;
        }
        if (type instanceof ParameterizedType) {
            return (Class<T>) ((ParameterizedType) type).getRawType();
        }

        throw new ClassCastException(String.format("Fail to resolve type %s to Class", type));
    }


    public static ClassTypeSignature toObjectType(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Class<?> raw = get(type);
            Type parent = pt.getOwnerType();
            if (parent == null) {
                return new ClassTypeSignature(raw.getName(), toTypeArguments(pt.getActualTypeArguments()));
            } else {
                return new ClassTypeSignature(toObjectType(parent), raw.getSimpleName(), toTypeArguments(pt.getActualTypeArguments()));
            }
        } else if (type instanceof Class) {
            Class<?> raw = (Class<?>) type;
            if (raw.getDeclaringClass() != null) {
                return new ClassTypeSignature(toObjectType(raw.getDeclaringClass()), raw.getSimpleName());
            } else {
                return new ClassTypeSignature(raw.getName());
            }
        }
        throw new IllegalArgumentException("unknown classType: " + type);
    }
}

