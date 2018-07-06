/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.compiler.runtime.FieldWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class RecordPropertyAdapter extends OpenPropertyAdapter {
    public RecordPropertyAdapter(TypeWidget type) {
        super(type);
    }

    public static void mergeFields(Record input, FieldWriter output) {
        if(input == null) {
            return;
        }
        for(String fieldName : input.getFieldNames()) {
            Object out = input.get(fieldName);
            if(out != null) {
                output.put(fieldName, out);
            }
        }
    }

    public static Object get(Record input, Object key) {
        if(input == null) {
            return null;
        }
        if(key instanceof String) {
            return input.get((String)key);
        }
        return null;
    }

    @Override
    public BytecodeExpression getPropertyNameIterable(BytecodeExpression target) {
        return new InvokeExpression(Record.class, "getFieldNames",
                new IterableTypeWidget(BaseTypeAdapter.STRING),
                target,
                ImmutableList.of());
    }

    @Override
    public AssignableValue property(BytecodeExpression target, String propertyName) {
        return index(target, new StringConstantExpression(propertyName));
    }

    @Override
    public AssignableValue index(final BytecodeExpression target, final BytecodeExpression propertyName) {
        return new ReadOnlyAssignableValue(AnyTypeWidget.getInstance()) {
            @Override
            public BytecodeExpression read() {
                return new InvokeExpression(RecordPropertyAdapter.class,
                        Opcodes.INVOKESTATIC,
                        "get",
                        Type.getMethodDescriptor(Type.getType(Object.class), Type.getType(Record.class), Type.getType(Object.class)),
                        AnyTypeWidget.getInstance(),
                        null,
                        ImmutableList.of(target, propertyName));
            }
        };
    }
}
