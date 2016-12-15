/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.IteratingSerializing;
import com.yahoo.yqlplus.engine.internal.plan.types.base.JavaIterableAdapter;
import org.objectweb.asm.Type;

public class IterableTypeWidget extends BaseTypeWidget {
    protected final TypeWidget valueType;

    public IterableTypeWidget(TypeWidget valueType) {
        this(Type.getType(Iterable.class), valueType);
    }

    public IterableTypeWidget(Type jvmType, TypeWidget valueType) {
        super(jvmType);
        this.valueType = valueType;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.ARRAY;
    }

    @Override
    public boolean isIterable() {
        return true;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new JavaIterableAdapter(valueType);
    }

    @Override
    protected SerializationAdapter getJsonSerializationAdapter() {
        return new IteratingSerializing(getIterableAdapter(), NativeEncoding.JSON);
    }
}
