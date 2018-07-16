/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.language.parser.Location;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

public class ReflectivePropertyAdapter extends ClosedPropertyAdapter {
    private final Map<String, PropertyReader> properties;

    public ReflectivePropertyAdapter(TypeWidget typeWidget, Map<String, PropertyReader> properties, List<Property> propertyList) {
        super(typeWidget, propertyList);
        this.properties = properties;
    }

    public static PropertyAdapter create(TypeWidget typeWidget, EngineValueTypeAdapter adapter, TypeLiteral<?> typeLiteral) {
        Map<String,PropertyReader> properties = readProperties(typeLiteral, adapter);
        ImmutableList.Builder<Property> propertyBuilder = ImmutableList.builder();
        for (PropertyReader reader : properties.values()) {
            propertyBuilder.add(reader.property);
        }
        List<Property> propertyList = propertyBuilder.build();
        return new ReflectivePropertyAdapter(typeWidget, properties, propertyList);
    }

    private static GambitCreator.Invocable findWriter(EngineValueTypeAdapter adapter, String name, TypeLiteral<?> owner, Method method) {
        try {
            Method writeMethod = method.getDeclaringClass().getMethod("set" + name, method.getReturnType());
            return ExactInvocation.reflectInvoke(adapter, owner, writeMethod);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }


    private static Map<String, PropertyReader> readProperties(TypeLiteral<?> typeLiteral, EngineValueTypeAdapter adapter) {
        Map<String, PropertyReader> builder = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Method method : typeLiteral.getRawType().getMethods()) {
            if (Object.class.equals(method.getDeclaringClass())) {
                continue;
            }
            if (Modifier.isStatic(method.getModifiers()) || !Modifier.isPublic(method.getModifiers())) {
                continue;
            }
            if (method.getParameterTypes().length > 0) {
                continue;
            }
            if (method.getName().startsWith("get") && method.getName().length() > 3) {
                String fieldName = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4);
                String methodSuffix = method.getName().substring(3);
                GambitCreator.Invocable invoker = ExactInvocation.reflectInvoke(adapter, typeLiteral, method);
                builder.put(fieldName, new MethodPropertyReader(fieldName, invoker, findWriter(adapter, methodSuffix, typeLiteral, method), invoker.getReturnType()));
            } else if (method.getName().startsWith("is") && method.getName().length() > 2 && (Boolean.class.isAssignableFrom(method.getReturnType()) || boolean.class.isAssignableFrom(method.getReturnType()))) {
                String fieldName = method.getName().substring(2, 3).toLowerCase() + method.getName().substring(3);
                String methodSuffix = method.getName().substring(2);
                GambitCreator.Invocable invoker = ExactInvocation.reflectInvoke(adapter, typeLiteral, method);
                builder.put(fieldName, new MethodPropertyReader(fieldName, invoker, findWriter(adapter, methodSuffix, typeLiteral, method), invoker.getReturnType()));
            }
        }
        for (Field field : typeLiteral.getRawType().getFields()) {
            if (Modifier.isStatic(field.getModifiers()) || !Modifier.isPublic(field.getModifiers())) {
                continue;
            }
            TypeLiteral<?> returnType = typeLiteral.getFieldType(field);
            TypeWidget ty = adapter.adapt(returnType);
            builder.put(field.getName(), new FieldPropertyReader(field, ty));
        }
        return builder;
    }

    abstract static class PropertyReader {
        Property property;

        PropertyReader(Property property) {
            this.property = property;
        }

        abstract public AssignableValue read(BytecodeExpression target);
    }

    @Override
    protected AssignableValue getPropertyValue(BytecodeExpression target, String propertyName) {
        PropertyReader reader = properties.get(propertyName);
        if (reader == null) {
            throw new PropertyNotFoundException("Type '%s' does not have property '%s'", type.getTypeName());
        }
        return reader.read(target);
    }

    private static class FieldPropertyReader extends PropertyReader {
        private final Field field;

        public FieldPropertyReader(Field field, TypeWidget ty) {
            super(new Property(field.getName(), ty));
            this.field = field;
        }

        @Override
        public AssignableValue read(BytecodeExpression target) {
            return new FieldAssignableValue(field, property.type, target);
        }
    }

    private static class MethodPropertyReader extends PropertyReader {
        private final GambitCreator.Invocable reader;
        private final GambitCreator.Invocable writer;

        public MethodPropertyReader(String name, GambitCreator.Invocable method, GambitCreator.Invocable writer, TypeWidget ty) {
            super(new Property(name, ty));
            this.reader = method;
            this.writer = writer;
        }

        @Override
        public AssignableValue read(final BytecodeExpression target) {
            return new AssignableValue() {
                @Override
                public TypeWidget getType() {
                    return reader.getReturnType();
                }

                @Override
                public BytecodeExpression read() {
                    return reader.invoke(Location.NONE, target);
                }

                @Override
                public BytecodeSequence write(BytecodeExpression value) {
                    if(writer == null) {
                        throw new UnsupportedOperationException();
                    }
                    return new BytecodeSequence() {
                        @Override
                        public void generate(CodeEmitter code) {
                            code.exec(writer.invoke(Location.NONE, target, value));
                        }
                    };
                }

                @Override
                public BytecodeSequence write(TypeWidget top) {
                    if(writer == null) {
                        throw new UnsupportedOperationException();
                    }
                    return new BytecodeSequence() {
                        @Override
                        public void generate(CodeEmitter code) {
                            AssignableValue stored = code.allocate(top);
                            code.exec(stored.write(top));
                            code.exec(writer.invoke(Location.NONE, target, stored));
                        }
                    };
                }

                @Override
                public void generate(CodeEmitter code) {
                    code.exec(read());
                }
            };
        }
    }
}
