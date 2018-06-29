/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.ImmutableMap;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Map;

public class Conversions {
    static String key(Class fromClass, Class toClass) {
        return Type.getDescriptor(fromClass) + "->" + Type.getDescriptor(toClass);
    }

    static String key(Type from, Type to) {
        return from.getDescriptor() + "->" + to.getDescriptor();
    }

    static class ConvertCall implements BytecodeSequence {
        final Type owner;
        final String methodName;
        final String descriptor;

        ConvertCall(Class owner, String methodName, Class returnType) {
            this.owner = Type.getType(owner);
            this.methodName = methodName;
            this.descriptor = Type.getMethodDescriptor(Type.getType(returnType));
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL, owner.getInternalName(), methodName, descriptor, false);
        }
    }

    static class BoxCall implements BytecodeSequence {
        final Type owner;
        final String methodName;
        final String descriptor;

        BoxCall(Class source, String methodName, Class returnType) {
            this.owner = Type.getType(returnType);
            this.methodName = methodName;
            this.descriptor = Type.getMethodDescriptor(Type.getType(returnType), Type.getType(source));
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC, owner.getInternalName(), methodName, descriptor, false);
        }
    }

    static class InstructionCall implements BytecodeSequence {
        final int opcode;

        InstructionCall(int opcode) {
            this.opcode = opcode;
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitInsn(opcode);
        }
    }

    static class InstructionsCall implements BytecodeSequence {
        final int opcode;
        final int[] opcodes;

        InstructionsCall(int opcode, int[] opcodes) {
            this.opcode = opcode;
            this.opcodes = opcodes;
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitInsn(opcode);
            for (int opcode : opcodes) {
                code.getMethodVisitor().visitInsn(opcode);
            }
        }
    }

    static void registerMethod(ImmutableMap.Builder<String, BytecodeSequence> m, Class from, Class to, String method) {
        m.put(key(from, to), new ConvertCall(from, method, to));
    }

    static void registerInsn(ImmutableMap.Builder<String, BytecodeSequence> m, Class from, Class to, int opcode) {
        m.put(key(from, to), new InstructionCall(opcode));
    }

    static void registerInsns(ImmutableMap.Builder<String, BytecodeSequence> m, Class from, Class to, int opcode, int... opcodes) {
        m.put(key(from, to), new InstructionsCall(opcode, opcodes));
    }

    static void registerNoop(ImmutableMap.Builder<String, BytecodeSequence> m, Class from, Class to) {
        m.put(key(from, to), BytecodeSequence.NOOP);
    }

    static void registerBox(ImmutableMap.Builder<String, BytecodeSequence> m, Class from, Class to, String method) {
        m.put(key(from, to), new BoxCall(from, method, to));
    }

    static final Map<String, BytecodeSequence> CONVERSIONS;

    static {
        ImmutableMap.Builder<String, BytecodeSequence> m = ImmutableMap.builder();
        registerBox(m, Boolean.TYPE, Boolean.class, "valueOf");
        registerMethod(m, Boolean.class, Boolean.TYPE, "booleanValue");
        registerBox(m, Character.TYPE, Character.class, "valueOf");
        registerBox(m, Byte.TYPE, Byte.class, "valueOf");
        registerBox(m, Short.TYPE, Short.class, "valueOf");
        registerBox(m, Integer.TYPE, Integer.class, "valueOf");
        registerBox(m, Long.TYPE, Long.class, "valueOf");
        registerBox(m, Float.TYPE, Float.class, "valueOf");
        registerBox(m, Double.TYPE, Double.class, "valueOf");

        registerMethod(m, Character.class, Character.TYPE, "charValue");

        registerSeq(m, Byte.TYPE, Short.TYPE, Short.class, "valueOf");
        registerSeq(m, Byte.TYPE, Integer.TYPE, Integer.class, "valueOf");
        registerSeq(m, Byte.TYPE, Opcodes.I2L, Long.TYPE, Long.class, "valueOf");
        registerSeq(m, Short.TYPE, Integer.TYPE, Integer.class, "valueOf");
        registerSeq(m, Short.TYPE, Opcodes.I2L, Long.TYPE, Long.class, "valueOf");
        registerSeq(m, Integer.TYPE, Opcodes.I2L, Long.TYPE, Long.class, "valueOf");
        registerSeq(m, Float.TYPE, Opcodes.F2D, Double.TYPE, Double.class, "valueOf");

        for (Class<?> numberClass : new Class<?>[]{Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Number.class}) {
            registerMethod(m, numberClass, Byte.TYPE, "byteValue");
            registerMethod(m, numberClass, Short.TYPE, "shortValue");
            registerMethod(m, numberClass, Integer.TYPE, "intValue");
            registerMethod(m, numberClass, Long.TYPE, "longValue");
            registerMethod(m, numberClass, Float.TYPE, "floatValue");
            registerMethod(m, numberClass, Double.TYPE, "doubleValue");
        }


        registerNoop(m, Boolean.TYPE, Character.TYPE);
        registerInsn(m, Boolean.TYPE, Byte.TYPE, Opcodes.I2B);
        registerInsn(m, Boolean.TYPE, Short.TYPE, Opcodes.I2S);
        registerNoop(m, Boolean.TYPE, Integer.TYPE);
        registerInsn(m, Boolean.TYPE, Long.TYPE, Opcodes.I2L);
        registerInsn(m, Boolean.TYPE, Float.TYPE, Opcodes.I2F);
        registerInsn(m, Boolean.TYPE, Double.TYPE, Opcodes.I2D);

        registerNoop(m, Character.TYPE, Boolean.TYPE);
        registerInsn(m, Character.TYPE, Byte.TYPE, Opcodes.I2B);
        registerInsn(m, Character.TYPE, Short.TYPE, Opcodes.I2S);
        registerNoop(m, Character.TYPE, Integer.TYPE);
        registerInsn(m, Character.TYPE, Long.TYPE, Opcodes.I2L);
        registerInsn(m, Character.TYPE, Float.TYPE, Opcodes.I2F);
        registerInsn(m, Character.TYPE, Double.TYPE, Opcodes.I2D);
        m.put(key(String.class, Character.TYPE), new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.emitIntConstant(0);
                        code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                Type.getInternalName(String.class),
                                "charAt",
                                Type.getMethodDescriptor(Type.CHAR_TYPE, Type.INT_TYPE),
                                false);
                    }
                });

                registerNoop(m, Byte.TYPE, Boolean.TYPE);
        registerNoop(m, Byte.TYPE, Short.TYPE);
        registerNoop(m, Byte.TYPE, Integer.TYPE);
        registerNoop(m, Byte.TYPE, Character.TYPE);
        registerInsn(m, Byte.TYPE, Long.TYPE, Opcodes.I2L);
        registerInsn(m, Byte.TYPE, Float.TYPE, Opcodes.I2F);
        registerInsn(m, Byte.TYPE, Double.TYPE, Opcodes.I2D);

        registerNoop(m, Short.TYPE, Boolean.TYPE);
        registerInsn(m, Short.TYPE, Byte.TYPE, Opcodes.I2S);
        registerNoop(m, Short.TYPE, Integer.TYPE);
        registerNoop(m, Short.TYPE, Character.TYPE);
        registerInsn(m, Short.TYPE, Long.TYPE, Opcodes.I2L);
        registerInsn(m, Short.TYPE, Float.TYPE, Opcodes.I2F);
        registerInsn(m, Short.TYPE, Double.TYPE, Opcodes.I2D);

        registerNoop(m, Integer.TYPE, Boolean.TYPE);
        registerInsn(m, Integer.TYPE, Byte.TYPE, Opcodes.I2B);
        registerInsn(m, Integer.TYPE, Short.TYPE, Opcodes.I2S);
        registerNoop(m, Integer.TYPE, Character.TYPE);
        registerInsn(m, Integer.TYPE, Long.TYPE, Opcodes.I2L);
        registerInsn(m, Integer.TYPE, Float.TYPE, Opcodes.I2F);
        registerInsn(m, Integer.TYPE, Double.TYPE, Opcodes.I2D);

        registerInsn(m, Long.TYPE, Boolean.TYPE, Opcodes.L2I);
        registerInsns(m, Long.TYPE, Byte.TYPE, Opcodes.L2I, Opcodes.I2B);
        registerInsns(m, Long.TYPE, Short.TYPE, Opcodes.L2I, Opcodes.I2S);
        registerInsn(m, Long.TYPE, Integer.TYPE, Opcodes.L2I);
        registerInsn(m, Long.TYPE, Character.TYPE, Opcodes.L2I);
        registerInsn(m, Long.TYPE, Float.TYPE, Opcodes.L2F);
        registerInsn(m, Long.TYPE, Double.TYPE, Opcodes.L2D);

        registerInsn(m, Float.TYPE, Boolean.TYPE, Opcodes.F2I);
        registerInsns(m, Float.TYPE, Byte.TYPE, Opcodes.F2I, Opcodes.I2B);
        registerInsns(m, Float.TYPE, Short.TYPE, Opcodes.F2I, Opcodes.I2S);
        registerInsn(m, Float.TYPE, Integer.TYPE, Opcodes.F2I);
        registerInsn(m, Float.TYPE, Character.TYPE, Opcodes.F2I);
        registerInsn(m, Float.TYPE, Long.TYPE, Opcodes.F2L);
        registerInsn(m, Float.TYPE, Double.TYPE, Opcodes.F2D);

        registerInsn(m, Double.TYPE, Boolean.TYPE, Opcodes.F2I);
        registerInsns(m, Double.TYPE, Byte.TYPE, Opcodes.D2I, Opcodes.I2B);
        registerInsns(m, Double.TYPE, Short.TYPE, Opcodes.D2I, Opcodes.I2S);
        registerInsn(m, Double.TYPE, Integer.TYPE, Opcodes.D2I);
        registerInsn(m, Double.TYPE, Character.TYPE, Opcodes.D2I);
        registerInsn(m, Double.TYPE, Long.TYPE, Opcodes.D2L);
        registerInsn(m, Double.TYPE, Float.TYPE, Opcodes.D2F);

        CONVERSIONS = m.build();
    }

    private static void registerSeq(ImmutableMap.Builder<String, BytecodeSequence> m, Class<?> fromType, final int opcode, Class<?> asType, Class<?> ownerType, String methodName) {
        final BoxCall box = new BoxCall(asType, methodName, ownerType);
        m.put(key(fromType, ownerType), new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.getMethodVisitor().visitInsn(opcode);
                code.exec(box);
            }
        });
    }

    private static void registerSeq(ImmutableMap.Builder<String, BytecodeSequence> m, Class<?> fromType, Class<?> type, Class<?> ownerType, String methodName) {
        m.put(key(fromType, ownerType), new BoxCall(type, methodName, ownerType));
    }

    public static BytecodeSequence getConversion(Type from, Type to) {
        return CONVERSIONS.get(key(from, to));
    }

}
