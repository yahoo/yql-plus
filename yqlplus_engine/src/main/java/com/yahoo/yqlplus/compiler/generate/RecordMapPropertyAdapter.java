/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.types.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.exprs.InvokeExpression;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Map;

public class RecordMapPropertyAdapter extends RecordPropertyAdapter {
    public RecordMapPropertyAdapter(TypeWidget type) {
        super(type);
    }

    @Override
    public AssignableValue index(final BytecodeExpression target, final BytecodeExpression propertyName) {
        return new AssignableValue() {
            @Override
            public TypeWidget getType() {
                return AnyTypeWidget.getInstance();
            }

            @Override
            public BytecodeSequence write(BytecodeExpression value) {
                return new PopExpression(new InvokeExpression(Map.class, Opcodes.INVOKEINTERFACE,
                        "put",
                        Type.getMethodDescriptor(Type.getType(Object.class), Type.getType(Object.class), Type.getType(Object.class)),
                        AnyTypeWidget.getInstance(),
                        target,
                        ImmutableList.of(propertyName, value)));
            }

            @Override
            public BytecodeSequence write(final TypeWidget top) {
                return new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        AssignableValue out = code.allocate(top);
                        out.write(top);
                        write(out);
                    }
                };
            }

            @Override
            public void generate(CodeEmitter code) {
                read().generate(code);
            }

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
