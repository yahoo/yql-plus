/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.plan.types.base.PropertyAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.StructBaseTypeWidget;
import org.objectweb.asm.Type;

public class RecordTypeWidget extends StructBaseTypeWidget {
    public RecordTypeWidget() {
        super(Type.getType(Record.class));
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return new RecordPropertyAdapter(this);
    }
}
