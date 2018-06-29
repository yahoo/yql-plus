/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import org.objectweb.asm.Type;

import java.util.Map;

public class MapEntryTypeWidget extends StructBaseTypeWidget {
    private final TypeWidget keyType;
    private final TypeWidget valueType;

    public MapEntryTypeWidget(TypeWidget keyType, TypeWidget valueType) {
        super(Type.getType(Map.Entry.class));
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return new ClosedPropertyAdapter(this, ImmutableList.of(
                new PropertyAdapter.Property("key", keyType),
                new PropertyAdapter.Property("value", valueType)
        )) {

            @Override
            protected AssignableValue getPropertyValue(BytecodeExpression target, String propertyName) {
                if ("key".equalsIgnoreCase(propertyName)) {
                    return new CastAssignableValue(keyType, new MethodAssignableValue(AnyTypeWidget.getInstance(), Map.Entry.class, "getKey", target));
                } else if ("value".equalsIgnoreCase(propertyName)) {
                    return new CastAssignableValue(valueType, new MethodAssignableValue(AnyTypeWidget.getInstance(), Map.Entry.class, "getValue", target));
                } else {
                    throw new PropertyNotFoundException("Map.Entry does not have property '%s'", propertyName);
                }
            }
        };
    }
}
