/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;

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
                TypeLiteral returnType = typeLiteral.getReturnType(method);
                TypeWidget ty = adapter.adaptInternal(returnType);
                builder.put(fieldName, new MethodPropertyReader(fieldName, method, ty));
            } else if (method.getName().startsWith("is") && method.getName().length() > 2 && (Boolean.class.isAssignableFrom(method.getReturnType()) || boolean.class.isAssignableFrom(method.getReturnType()))) {
                String fieldName = method.getName().substring(2, 3).toLowerCase() + method.getName().substring(3);
                TypeLiteral returnType = typeLiteral.getReturnType(method);
                TypeWidget ty = adapter.adaptInternal(returnType);
                builder.put(fieldName, new MethodPropertyReader(fieldName, method, ty));
            }
        }
        for (Field field : typeLiteral.getRawType().getFields()) {
            if (Modifier.isStatic(field.getModifiers()) || !Modifier.isPublic(field.getModifiers())) {
                continue;
            }
            TypeLiteral<?> returnType = typeLiteral.getFieldType(field);
            TypeWidget ty = adapter.adaptInternal(returnType);
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
        private final Method method;

        public MethodPropertyReader(String name, Method method, TypeWidget ty) {
            super(new Property(name, ty));
            this.method = method;
        }

        @Override
        public AssignableValue read(BytecodeExpression target) {
            return new MethodAssignableValue(method, property.type, target);
        }
    }
}
