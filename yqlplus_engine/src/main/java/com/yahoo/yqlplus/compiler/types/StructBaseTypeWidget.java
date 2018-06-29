/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.compiler.code.IndexAdapter;
import org.objectweb.asm.Type;

public abstract class StructBaseTypeWidget extends BaseTypeWidget {
    public StructBaseTypeWidget(Type type) {
        super(type);
    }

    @Override
    public final YQLCoreType getValueCoreType() {
        return YQLCoreType.STRUCT;
    }

    @Override
    public final boolean hasProperties() {
        return true;
    }

    @Override
    public abstract PropertyAdapter getPropertyAdapter();

    @Override
    public final IndexAdapter getIndexAdapter() {
        return new StructIndexAdapter(this, getPropertyAdapter());
    }

    @Override
    public final boolean isIndexable() {
        return true;
    }
}
