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
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class IteratingSerializing implements SerializationAdapter {
    private final IterateAdapter iterateAdapter;
    private final NativeEncoding encoding;
    private final SerializationAdapter valueTypeSerializer;

    public IteratingSerializing(IterateAdapter iterableAdapter, NativeEncoding encoding) {
        this.iterateAdapter = iterableAdapter;
        this.encoding = encoding;
        this.valueTypeSerializer = iterateAdapter.getValue().getSerializationAdapter(encoding);
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
                        gen.generate(code);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeStartArray", Type.getMethodDescriptor(Type.VOID_TYPE), false);
                        code.exec(iterateAdapter.iterate(source, new IterateAdapter.IterateLoop() {
                            @Override
                            public void item(CodeEmitter code, BytecodeExpression item, Label abortLoop, Label nextItem) {
                                code.exec(valueTypeSerializer.serializeTo(item, gen));
                            }
                        }));
                        gen.generate(code);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeEndArray", Type.getMethodDescriptor(Type.VOID_TYPE), false);
                    }
                };
            case TBIN:
                // TODO: implement iterable TBin serialization (whoops, it requires you to know the count to write an array... hm?)
                // we may need to copy into a list
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
