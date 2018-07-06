/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Map;

public class MapPropertyAdapter extends OpenPropertyAdapter {
    private final TypeWidget keyType;
    private final TypeWidget valueType;

    public MapPropertyAdapter(TypeWidget mapTypeWidget, TypeWidget keyType, TypeWidget valueType) {
        super(mapTypeWidget);
        this.keyType = keyType;
        this.valueType = NullableTypeWidget.create(valueType);
    }

    public TypeWidget getPropertyType(String propertyName) {
        return NullableTypeWidget.create(valueType);
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression) {
        return new MapAssignableValue(valueType, target, indexExpression);
    }

    public static Iterable<Object> stringsOnly(Iterable<Object> input) {
        return Iterables.filter(input, new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return input != null && input instanceof String;
            }
        });
    }

    @Override
    public BytecodeExpression getPropertyNameIterable(BytecodeExpression target) {
        return new InvokeExpression(MapPropertyAdapter.class,
                Opcodes.INVOKESTATIC,
                "stringsOnly",
                Type.getMethodDescriptor(Type.getType(Iterable.class), Type.getType(Iterable.class)),
                new IterableTypeWidget(BaseTypeAdapter.STRING),
                null,
                ImmutableList.of(new InvokeExpression(Map.class,
                "keySet",
                new SetTypeWidget(keyType),
                target,
                ImmutableList.of())));
    }
}
