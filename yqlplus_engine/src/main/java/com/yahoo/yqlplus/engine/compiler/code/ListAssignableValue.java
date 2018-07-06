/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

class ListAssignableValue implements AssignableValue {
    private final TypeWidget valueType;
    private final BytecodeExpression target;
    private final BytecodeExpression indexExpression;

    public ListAssignableValue(TypeWidget valueType, BytecodeExpression target, BytecodeExpression indexExpression) {
        this.valueType = valueType;
        this.target = target;
        this.indexExpression = indexExpression;
    }

    @Override
    public TypeWidget getType() {
        return valueType;
    }

    @Override
    public BytecodeExpression read() {
        return new BaseTypeExpression(valueType) {
            @Override
            public void generate(CodeEmitter code) {
                target.generate(code);
                indexExpression.generate(code);
                code.cast(BaseTypeAdapter.INT32, indexExpression.getType());
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(List.class), "get", Type.getMethodDescriptor(Type.getType(Object.class), Type.INT_TYPE), true);
                code.cast(valueType, BaseTypeAdapter.ANY);
            }
        };
    }

    @Override
    public BytecodeSequence write(final BytecodeExpression value) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                target.generate(code);
                indexExpression.generate(code);
                code.cast(BaseTypeAdapter.INT32, indexExpression.getType());
                value.generate(code);
                code.box(value.getType());
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(List.class), "set", Type.getMethodDescriptor(Type.getType(Object.class), Type.INT_TYPE, Type.getType(Object.class)), true);
                code.pop(BaseTypeAdapter.ANY);
            }
        };
    }

    @Override
    public BytecodeSequence write(final TypeWidget top) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.box(top);
                // value -> value, target
                target.generate(code);
                // value, target -> target, value
                code.swap(top.boxed(), target.getType());
                // target, value -> target, value, index
                indexExpression.generate(code);
                code.cast(BaseTypeAdapter.INT32, indexExpression.getType());
                // target, value, index -> target, index, value
                code.swap(BaseTypeAdapter.INT32, top.boxed());
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(List.class), "set", Type.getMethodDescriptor(Type.getType(Object.class), Type.INT_TYPE, Type.getType(Object.class)), true);
                code.pop(BaseTypeAdapter.ANY);
            }
        };
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }
}
