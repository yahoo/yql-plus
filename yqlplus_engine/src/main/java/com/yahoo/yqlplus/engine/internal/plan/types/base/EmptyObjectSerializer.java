/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class EmptyObjectSerializer implements SerializationAdapter {
    private final NativeEncoding encoding;

    public EmptyObjectSerializer(TypeWidget targetType, NativeEncoding encoding) {
        this.encoding = encoding;
    }

    @Override
    public BytecodeSequence serializeTo(final BytecodeExpression source, final BytecodeExpression generator) {
        switch(encoding) {
            case JSON:
                return new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        MethodVisitor mv = code.getMethodVisitor();
                        final BytecodeExpression gen = code.evaluateOnce(generator);
                        code.exec(gen);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeStartObject", Type.getMethodDescriptor(Type.VOID_TYPE), false);
                        code.exec(gen);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeEndObject", Type.getMethodDescriptor(Type.VOID_TYPE), false);
                    }
                };
            case TBIN:
                throw new TodoException();
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        throw new UnsupportedOperationException();
    }
}
