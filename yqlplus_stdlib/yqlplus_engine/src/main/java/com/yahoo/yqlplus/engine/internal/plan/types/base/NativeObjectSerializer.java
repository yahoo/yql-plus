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
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class NativeObjectSerializer implements SerializationAdapter {
    private final PropertyAdapter propertyAdapter;
    private final NativeEncoding encoding;

    public NativeObjectSerializer(PropertyAdapter propertyAdapter, NativeEncoding encoding) {
        this.propertyAdapter = propertyAdapter;
        this.encoding = encoding;
    }

    @Override
    public BytecodeSequence serializeTo(final BytecodeExpression source, final BytecodeExpression generator) {
        switch (encoding) {
            case JSON:
                return new JsonSerialize(source, generator);
            case TBIN:
                throw new TodoException();
            default:
                throw new UnsupportedOperationException("Unknown encoding: " + encoding);
        }

    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        throw new UnsupportedOperationException();
    }

    private abstract class ObjectSerialize implements BytecodeSequence {
        protected final BytecodeExpression source;
        protected final BytecodeExpression generator;

        public ObjectSerialize(BytecodeExpression source, BytecodeExpression generator) {
            this.source = source;
            this.generator = generator;
        }

        @Override
        public void generate(CodeEmitter top) {
            CodeEmitter code = top.createScope();
            MethodVisitor mv = code.getMethodVisitor();
            enterObject(code, source.getType());
            code.exec(propertyAdapter.visitProperties(source, new PropertyAdapter.PropertyVisit() {
                @Override
                public void item(CodeEmitter code, BytecodeExpression propertyName, BytecodeExpression propertyValue, Label abortLoop, Label nextItem) {
                    writeField(code, propertyName);
                    code.exec(propertyValue.getType().getSerializationAdapter(encoding).serializeTo(propertyValue, generator));
                }
            }));
            exitObject(code, source.getType());
            code.endScope();
        }

        protected abstract void enterObject(CodeEmitter code, TypeWidget type);
        protected abstract void writeField(CodeEmitter code, String name);
        protected abstract void writeField(CodeEmitter code, BytecodeExpression name);
        protected abstract void exitObject(CodeEmitter code, TypeWidget type);

    }

    private class JsonSerialize extends ObjectSerialize {

        public JsonSerialize(BytecodeExpression source, BytecodeExpression generator) {
            super(source, generator);
        }

        @Override
        protected void enterObject(CodeEmitter code, TypeWidget type) {
            code.exec(generator);
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeStartObject", Type.getMethodDescriptor(Type.VOID_TYPE), false);
        }

        @Override
        protected void exitObject(CodeEmitter code, TypeWidget type) {
            code.exec(generator);
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeEndObject", Type.getMethodDescriptor(Type.VOID_TYPE), false);
        }

        @Override
        protected void writeField(CodeEmitter code, String name) {
            writeField(code, new StringConstantExpression(name));
        }

        @Override
        protected void writeField(CodeEmitter code, BytecodeExpression name) {
            code.exec(generator);
            code.exec(name);
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeFieldName", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(String.class)), false);
        }

    }
}
