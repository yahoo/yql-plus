/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import org.objectweb.asm.Type;

import java.util.List;

public class KeyCursorTypeWidget extends BaseTypeWidget {
    private final List<String> names;
    private final TypeWidget valueType;

    public KeyCursorTypeWidget(Type type, List<String> names, TypeWidget valueType) {
        super(type);
        this.names = names;
        this.valueType = valueType;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    public List<String> getNames() {
        return names;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.OBJECT;
    }


    @Override
    public boolean isIterable() {
        return true;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new JavaIterableAdapter(valueType);
    }

}
