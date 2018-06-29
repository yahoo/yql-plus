/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.compiler.runtime.RecordMapWrapper;
import org.objectweb.asm.Type;

public class DynamicRecordWidget extends StructBaseTypeWidget {
    public DynamicRecordWidget() {
        super(Type.getType(RecordMapWrapper.class));
    }

    public DynamicRecordWidget(Type type) {
        super(type);
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return new RecordMapPropertyAdapter(this);
    }
}
