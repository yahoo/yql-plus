/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class ArrayIndexAdapter implements IndexAdapter, IterateAdapter {
    private final TypeWidget ownerType;
    private final TypeWidget valueType;

    public ArrayIndexAdapter(TypeWidget ownerType, TypeWidget valueType) {
        this.ownerType = ownerType;
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getKey() {
        return BaseTypeAdapter.INT32;
    }

    @Override
    public TypeWidget getValue() {
        return valueType;
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression) {
        return new ArrayAssignableValue(getValue(), target, indexExpression);
    }

    @Override
    public BytecodeExpression length(final BytecodeExpression inputExpr) {
        return new BaseTypeExpression(BaseTypeAdapter.INT32) {
            @Override
            public void generate(CodeEmitter code) {
                Label done = new Label();
                Label isNull = new Label();
                MethodVisitor mv = code.getMethodVisitor();
                code.exec(inputExpr);
                boolean nullable = code.nullTest(inputExpr.getType(), isNull);
                mv.visitInsn(Opcodes.ARRAYLENGTH);
                if (nullable) {
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                    mv.visitLabel(isNull);
                    mv.visitInsn(Opcodes.ICONST_0);
                    mv.visitLabel(done);
                }
            }
        };
    }

    @Override
    public BytecodeSequence iterate(final BytecodeExpression target, final AssignableValue item, final IterateLoop loop) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter start) {
                CodeEmitter code = start.createScope();
                final AssignableValue count = code.allocate(BaseTypeAdapter.INT32);
                final AssignableValue idx = code.allocate(BaseTypeAdapter.INT32);
                Label done = new Label();
                Label next = new Label();
                MethodVisitor mv = code.getMethodVisitor();
                BytecodeExpression tgt = code.evaluateOnce(target);
                code.exec(tgt);
                code.nullTest(tgt.getType(), done);
                mv.visitInsn(Opcodes.ARRAYLENGTH);
                code.exec(count.write(BaseTypeAdapter.INT32));
                code.emitIntConstant(0);
                code.exec(idx.write(BaseTypeAdapter.INT32));
                mv.visitLabel(next);
                code.exec(count.read());
                mv.visitJumpInsn(Opcodes.IFEQ, done);
                code.inc(count, -1);
                code.exec(tgt);
                code.exec(idx.read());
                code.getMethodVisitor().visitInsn(ownerType.getJVMType().getOpcode(Opcodes.IALOAD));
                code.inc(idx, 1);
                code.nullTest(valueType, next);
                code.exec(item.write(item.getType()));
                loop.item(code, item.read(), done, next);
                mv.visitJumpInsn(Opcodes.GOTO, next);
                mv.visitLabel(done);
                code.endScope();
            }
        };
    }

    @Override
    public BytecodeExpression first(final BytecodeExpression target) {
        // what should this return if the array is of length 0?
        return new BaseTypeExpression(NullableTypeWidget.create(valueType.boxed())) {
            @Override
            public void generate(CodeEmitter start) {
                CodeEmitter code = start.createScope();
                Label done = new Label();
                Label isNull = new Label();
                MethodVisitor mv = code.getMethodVisitor();
                BytecodeExpression tgt = code.evaluateOnce(target);
                code.exec(tgt);
                code.nullTest(tgt.getType(), isNull);
                mv.visitInsn(Opcodes.ARRAYLENGTH);
                mv.visitJumpInsn(Opcodes.IFEQ, isNull);
                code.exec(tgt);
                code.emitIntConstant(0);
                mv.visitInsn(ownerType.getJVMType().getOpcode(Opcodes.IALOAD));
                code.cast(valueType.boxed(), valueType, isNull);
                mv.visitJumpInsn(Opcodes.GOTO, done);
                mv.visitLabel(isNull);
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitLabel(done);
                code.endScope();
            }
        };
    }

    @Override
    public BytecodeSequence iterate(final BytecodeExpression target, final IterateLoop loop) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter start) {
                CodeEmitter code = start.createScope();
                final AssignableValue item = code.allocate(valueType);
                code.exec(iterate(target, item, loop));
                code.endScope();
            }
        };
    }
}
