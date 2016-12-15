/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.bytecode.types.JVMTypes;
import com.yahoo.yqlplus.engine.internal.java.types.DynamicRecordWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.TypeAdaptingWidget;

import java.lang.reflect.Type;
import java.util.Map;

public class RecordAdaptingWidget implements TypeAdaptingWidget {
    @Override
    public boolean supports(Class<?> clazzType) {
        return Record.class.isAssignableFrom(clazzType);
    }

    @Override
    public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
        Class<?> clazz = JVMTypes.getRawType(type);
        if(Map.class.isAssignableFrom(clazz)) {
            return new DynamicRecordWidget(org.objectweb.asm.Type.getType(clazz));
        }
        return new RecordTypeWidget();
    }
}
