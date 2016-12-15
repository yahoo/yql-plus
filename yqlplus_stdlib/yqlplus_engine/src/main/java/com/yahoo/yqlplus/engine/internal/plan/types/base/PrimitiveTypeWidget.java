/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class PrimitiveTypeWidget extends BaseTypeWidget {
    YQLCoreType coreType;
    TypeWidget boxed;

    public PrimitiveTypeWidget(Type type, YQLCoreType coreType) {
        super(type);
        this.coreType = coreType;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return coreType;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public TypeWidget boxed() {
        return boxed;
    }

    @Override
    public TypeWidget unboxed() {
        return this;
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return new ComparisonAdapter() {
            @Override
            public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
                MethodVisitor mv = scope.getMethodVisitor();
                switch (getJVMType().getSort()) {
                    case Type.BOOLEAN:
                    case Type.SHORT:
                    case Type.INT:
                    case Type.CHAR:
                        mv.visitJumpInsn(Opcodes.IFEQ, isFalse);
                        mv.visitJumpInsn(Opcodes.GOTO, isTrue);
                        break;
                    case Type.FLOAT:
                        mv.visitInsn(Opcodes.FCONST_0);
                        mv.visitInsn(Opcodes.FCMPG);
                        mv.visitJumpInsn(Opcodes.IFEQ, isTrue);
                        mv.visitJumpInsn(Opcodes.GOTO, isFalse);
                        break;
                    case Type.LONG:
                        mv.visitInsn(Opcodes.LCONST_0);
                        mv.visitInsn(Opcodes.LCMP);
                        mv.visitJumpInsn(Opcodes.IFEQ, isTrue);
                        mv.visitJumpInsn(Opcodes.GOTO, isFalse);
                        break;
                    case Type.DOUBLE:
                        mv.visitInsn(Opcodes.DCONST_0);
                        mv.visitInsn(Opcodes.DCMPG);
                        mv.visitJumpInsn(Opcodes.IFEQ, isTrue);
                        mv.visitJumpInsn(Opcodes.GOTO, isFalse);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported JVM type: " + getJVMType());

                }
            }
        };
    }

    @Override
    public String toString() {
        return "<PrimitiveType:" + getJVMType().getDescriptor() + ">";
    }

    @Override
    protected SerializationAdapter getJsonSerializationAdapter() {
        return new SerializationAdapter() {
            @Override
            public BytecodeSequence serializeTo(BytecodeExpression source, BytecodeExpression generator) {
                return new JacksonSerializePrimitive(source, generator);
            }

            @Override
            public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
                return new JacksonParsePrimitive(PrimitiveTypeWidget.this, parser);
            }
        };
    }
}
