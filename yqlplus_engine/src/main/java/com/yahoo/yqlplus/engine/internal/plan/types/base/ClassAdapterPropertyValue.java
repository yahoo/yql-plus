/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.bytecode.ClassAdapterGenerator;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class ClassAdapterPropertyValue implements AssignableValue {
    private final TypeWidget typeWidget;
    private final BytecodeExpression target;
    private final BytecodeExpression indexExpression;
    private final ClassAdapterGenerator adapter;

    ClassAdapterPropertyValue(TypeWidget typeWidget, BytecodeExpression target, BytecodeExpression indexExpression, ClassAdapterGenerator adapter) {
        this.typeWidget = typeWidget;
        this.target = target;
        this.indexExpression = indexExpression;
        this.adapter = adapter;
    }

    @Override
    public TypeWidget getType() {
        return AnyTypeWidget.getInstance();
    }

    @Override
    public BytecodeExpression read() {
        return new BaseTypeExpression(getType()) {
            @Override
            public void generate(CodeEmitter code) {
                target.generate(code);
                indexExpression.generate(code);
                code.cast(BaseTypeAdapter.STRING, indexExpression.getType());
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC, adapter.getInternalName(), "getProperty",
                        Type.getMethodDescriptor(AnyTypeWidget.getInstance().getJVMType(), typeWidget.getJVMType(), BaseTypeAdapter.STRING.getJVMType()), false);
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
                code.cast(BaseTypeAdapter.STRING, indexExpression.getType());
                value.generate(code);
                code.box(value.getType());
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC, adapter.getInternalName(), "putProperty",
                        Type.getMethodDescriptor(Type.VOID_TYPE, typeWidget.getJVMType(), BaseTypeAdapter.STRING.getJVMType(), AnyTypeWidget.getInstance().getJVMType()), false);

            }
        };
    }

    @Override
    public BytecodeSequence write(final TypeWidget top) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                target.generate(code);
                code.swap(top, target.getType());
                indexExpression.generate(code);
                code.cast(BaseTypeAdapter.STRING, indexExpression.getType());
                code.swap(BaseTypeAdapter.STRING, top);
                code.box(top);
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC, adapter.getInternalName(), "putProperty",
                        Type.getMethodDescriptor(Type.VOID_TYPE, typeWidget.getJVMType(), BaseTypeAdapter.STRING.getJVMType(), AnyTypeWidget.getInstance().getJVMType()), false);
            }
        };
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }
}
