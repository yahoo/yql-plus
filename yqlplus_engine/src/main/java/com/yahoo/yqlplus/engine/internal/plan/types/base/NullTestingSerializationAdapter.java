/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;
import com.yahoo.tbin.TBinEncoder;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Type;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;


public class NullTestingSerializationAdapter implements SerializationAdapter {
    private final TypeWidget nullableTarget;
    private final NativeEncoding encoding;
    private final SerializationAdapter targetAdapter;

    public NullTestingSerializationAdapter(TypeWidget target, NativeEncoding encoding) {
        this(NullableTypeWidget.create(target), target, encoding);
    }

    public NullTestingSerializationAdapter(TypeWidget nullableTarget, TypeWidget target, NativeEncoding encoding) {
        Preconditions.checkArgument(nullableTarget.isNullable());
        Preconditions.checkArgument(!target.isNullable());
        this.nullableTarget = nullableTarget;
        this.encoding = encoding;
        this.targetAdapter = target.getSerializationAdapter(encoding);
    }


    @Override
    public BytecodeSequence serializeTo(final BytecodeExpression input, final BytecodeExpression generator) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label isNull = new Label();
                Label done = new Label();
                BytecodeExpression source = code.evaluateOnce(input);
                code.gotoIfNull(source, isNull);
                MethodVisitor mv = code.getMethodVisitor();
                code.exec(targetAdapter.serializeTo(source, generator));
                mv.visitJumpInsn(Opcodes.GOTO, done);
                mv.visitLabel(isNull);
                switch(encoding) {
                    case TBIN:
                        code.exec(generator);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                Type.getInternalName(TBinEncoder.class),
                                "writeNull",
                                Type.getMethodDescriptor(Type.VOID_TYPE),
                                false);
                        break;
                    case JSON:
                        code.exec(generator);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                Type.getInternalName(JsonGenerator.class),
                                "writeNull",
                                Type.getMethodDescriptor(Type.VOID_TYPE),
                                false);
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown NativeEncoding: " + encoding);
                }
                mv.visitLabel(done);
            }
        };
    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        return targetAdapter.deserializeFrom(parser);
    }
}
