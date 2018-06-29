/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.plan.types.*;
import org.objectweb.asm.Type;

import java.util.Map;

public class MapTypeWidget extends BaseTypeWidget {
    private final TypeWidget keyType;
    private final TypeWidget valueType;

    public MapTypeWidget(Type type, TypeWidget keyType, TypeWidget valueType) {
        super(type);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public MapTypeWidget(TypeWidget keyType, TypeWidget valueType) {
        super(Type.getType(Map.class));
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.MAP;
    }

    @Override
    public boolean hasProperties() {
        return keyType.getValueCoreType() == YQLCoreType.STRING || keyType.getValueCoreType() == YQLCoreType.ANY;
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return new MapPropertyAdapter(this, keyType, valueType);
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return new MapIndexAdapter(this, keyType, valueType);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return true;
    }

    @Override
    public boolean isIterable() {
        return true;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new MapIterateAdapter(keyType, valueType);
    }

    @Override
    public UnificationAdapter getUnificationAdapter(final ProgramValueTypeAdapter typeAdapter) {
        // this is only called when the two types match their JVM type
        return new UnificationAdapter() {
            @Override
            public TypeWidget unify(TypeWidget other) {
                IndexAdapter idxAdapter = other.getIndexAdapter();
                return new MapTypeWidget(getJVMType(), typeAdapter.unifyTypes(keyType, idxAdapter.getKey()), typeAdapter.unifyTypes(valueType, idxAdapter.getValue()));
            }
        };
    }


    static class MapIterateAdapter implements IterateAdapter {
        private final JavaIterableAdapter entryIterator;

        public MapIterateAdapter(TypeWidget keyType, TypeWidget valueType) {
            this.entryIterator = new JavaIterableAdapter(new MapEntryTypeWidget(keyType, valueType));
        }

        @Override
        public TypeWidget getValue() {
            return entryIterator.getValue();
        }

        @Override
        public BytecodeSequence iterate(final BytecodeExpression target, IterateLoop loop) {
            return entryIterator.iterate(new InvokeExpression(Map.class, "entrySet", new SetTypeWidget(entryIterator.getValue()), target, ImmutableList.of()), loop);
        }

        @Override
        public BytecodeSequence iterate(BytecodeExpression target, AssignableValue item, IterateLoop loop) {
            return entryIterator.iterate(new InvokeExpression(Map.class, "entrySet", new SetTypeWidget(entryIterator.getValue()), target, ImmutableList.of()), item, loop);
        }

        @Override
        public BytecodeExpression first(BytecodeExpression target) {
            return entryIterator.first(new InvokeExpression(Map.class, "entrySet", new SetTypeWidget(entryIterator.getValue()), target, ImmutableList.of()));
        }
    }

}
