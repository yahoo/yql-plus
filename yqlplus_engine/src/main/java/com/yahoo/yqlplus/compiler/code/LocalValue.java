/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.compiler.exprs.EvaluatedExpression;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Opcodes;

import java.util.List;

public class LocalValue implements AssignableValue, BytecodeExpression, EvaluatedExpression {
    private final List<Location> allocationLocation;
    TypeWidget type;
    String name;
    int start;

    public LocalValue(String name, TypeWidget type, int start) {
        this.allocationLocation = Lists.newArrayList();
        for (StackTraceElement elt : Thread.currentThread().getStackTrace()) {
            allocationLocation.add(new Location(elt.getFileName(), elt.getLineNumber(), 0));
        }
        this.name = name;
        this.type = type;
        this.start = start;
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    @Override
    public BytecodeSequence write(final BytecodeExpression value) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.checkFrame(LocalValue.this);
                value.generate(code);
                code.getMethodVisitor().visitVarInsn(type.getJVMType().getOpcode(Opcodes.ISTORE), start);
            }
        };
    }

    @Override
    public BytecodeSequence write(final TypeWidget top) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.checkFrame(LocalValue.this);
                code.cast(type, top);
                code.getMethodVisitor().visitVarInsn(type.getJVMType().getOpcode(Opcodes.ISTORE), start);
            }
        };
    }

    @Override
    public BytecodeExpression read() {
        return new LocalValueExpression(type) {
            @Override
            public void generate(CodeEmitter code) {
                code.checkFrame(LocalValue.this);
                code.getMethodVisitor().visitVarInsn(type.getJVMType().getOpcode(Opcodes.ILOAD), start);
            }
        };
    }

    @Override
    public void generate(CodeEmitter code) {
        read().generate(code);
    }
}
