/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.Opcodes;

public class ArrayAssignableValue implements AssignableValue {
    private final TypeWidget valueType;
    private final BytecodeExpression target;
    private final BytecodeExpression indexExpression;

    public ArrayAssignableValue(TypeWidget valueType, BytecodeExpression target, BytecodeExpression indexExpression) {
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
                code.getMethodVisitor().visitInsn(target.getType().getJVMType().getOpcode(Opcodes.IALOAD));
            }
        };
    }

    @Override
    public BytecodeSequence write(final BytecodeExpression value) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(target);
                code.exec(indexExpression);
                code.cast(BaseTypeAdapter.INT32, indexExpression.getType());
                code.exec(value);
                code.cast(valueType, value.getType());
                code.getMethodVisitor().visitInsn(target.getType().getJVMType().getOpcode(Opcodes.IASTORE));
            }
        };
    }

    @Override
    public BytecodeSequence write(final TypeWidget top) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.cast(valueType, top);
                // value -> value, target
                target.generate(code);
                // value, target -> target, value
                code.swap(valueType, target.getType());
                // target, value -> target, value, index
                indexExpression.generate(code);
                code.cast(BaseTypeAdapter.INT32, indexExpression.getType());
                // target, value, index -> target, index, value
                code.swap(BaseTypeAdapter.INT32, valueType);
                code.getMethodVisitor().visitInsn(target.getType().getJVMType().getOpcode(Opcodes.IASTORE));
            }
        };
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }

}
