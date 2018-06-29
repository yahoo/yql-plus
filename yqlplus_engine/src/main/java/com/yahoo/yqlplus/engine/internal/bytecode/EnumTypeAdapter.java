/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeWidget;
import org.objectweb.asm.Type;

public class EnumTypeAdapter extends BaseTypeWidget {
    public EnumTypeAdapter(Class<?> enumType) {
        super(Type.getType(enumType));
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.STRING;
    }

}
