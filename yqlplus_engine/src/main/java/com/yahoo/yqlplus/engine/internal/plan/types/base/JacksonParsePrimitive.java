/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.yahoo.yqlplus.engine.internal.bytecode.InstructionConstant;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.LocalBatchCode;
import com.yahoo.yqlplus.engine.internal.compiler.LocalCodeChunk;
import com.yahoo.yqlplus.engine.internal.compiler.LocalFrame;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class JacksonParsePrimitive extends BaseTypeExpression {
    private final BytecodeExpression parser;

    public JacksonParsePrimitive(TypeWidget typeWidget, BytecodeExpression parser) {
        super(typeWidget);
        this.parser = parser;
    }

    private static class JacksonGet implements BytecodeSequence {
        private Type returnType;
        private String name;

        public JacksonGet(Type returnType, String name) {
            this.returnType = returnType;
            this.name = name;
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(JsonParser.class),
                    name,
                    Type.getMethodDescriptor(returnType),
                    false);
        }
    }

    @Override
    public void generate(CodeEmitter code) {
        parser.generate(code);
        MethodVisitor mv = code.getMethodVisitor();
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(JsonParser.class),
                "nextToken",
                Type.getMethodDescriptor(Type.getType(JsonToken.class)),
                false);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(JsonToken.class),
                "ordinal",
                Type.getMethodDescriptor(Type.INT_TYPE),
                false);
        IntegerSwitchSequence seq = new IntegerSwitchSequence();
        switch (getType().getJVMType().getSort()) {
            case Type.BOOLEAN: {
                seq.put(JsonToken.VALUE_TRUE.ordinal(), new InstructionConstant(getType(), -1));
                seq.put(JsonToken.VALUE_FALSE.ordinal(), new InstructionConstant(getType(), 0));
                break;
            }
            case Type.SHORT: {
                seq.put(JsonToken.VALUE_NUMBER_INT.ordinal(), new JacksonGet(Type.SHORT_TYPE, "getShortValue"));
                seq.put(JsonToken.VALUE_NUMBER_FLOAT.ordinal(), new JacksonGet(Type.SHORT_TYPE, "getShortValue"));
                break;
            }
            case Type.INT: {
                seq.put(JsonToken.VALUE_NUMBER_INT.ordinal(), new JacksonGet(Type.INT_TYPE, "getIntValue"));
                seq.put(JsonToken.VALUE_NUMBER_FLOAT.ordinal(), new JacksonGet(Type.INT_TYPE, "getIntValue"));
                break;
            }
            case Type.FLOAT: {
                seq.put(JsonToken.VALUE_NUMBER_INT.ordinal(), new JacksonGet(Type.FLOAT_TYPE, "getFloatValue"));
                seq.put(JsonToken.VALUE_NUMBER_FLOAT.ordinal(), new JacksonGet(Type.FLOAT_TYPE, "getFloatValue"));
                break;
            }
            case Type.LONG: {
                seq.put(JsonToken.VALUE_NUMBER_INT.ordinal(), new JacksonGet(Type.LONG_TYPE, "getLongValue"));
                seq.put(JsonToken.VALUE_NUMBER_FLOAT.ordinal(), new JacksonGet(Type.LONG_TYPE, "getLongValue"));
                break;
            }
            case Type.DOUBLE: {
                seq.put(JsonToken.VALUE_NUMBER_INT.ordinal(), new JacksonGet(Type.DOUBLE_TYPE, "getDoubleValue"));
                seq.put(JsonToken.VALUE_NUMBER_FLOAT.ordinal(), new JacksonGet(Type.DOUBLE_TYPE, "getDoubleValue"));
                break;
            }
            case Type.CHAR: {
                LocalCodeChunk chunk = new LocalBatchCode(new LocalFrame());
                chunk.add(new JacksonGet(Type.getType(String.class), "getText"));
                chunk.add(new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        MethodVisitor mv = code.getMethodVisitor();
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(String.class), "charAt",
                                Type.getMethodDescriptor(Type.CHAR_TYPE, Type.INT_TYPE), false);
                    }
                });
                seq.put(JsonToken.VALUE_STRING.ordinal(), chunk);
                break;
            }
            default:
                throw new UnsupportedOperationException("Unknown primitive type: " + getType().getJVMType());
        }
        seq.setDefaultSequence(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.emitThrow(IllegalArgumentException.class, new StringConstantExpression(String.format("JSON type cannot be parsed into expected type '%s'", getType().getTypeName())));

            }
        });
        code.exec(seq);
    }
}
