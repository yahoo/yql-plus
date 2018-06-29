/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

public class InvokeExpression implements BytecodeExpression {
    private static String getDescriptor(TypeWidget returnType, List<BytecodeExpression> arguments) {
        Type[] argTypes = new Type[arguments.size()];
        for (int i = 0; i < arguments.size(); ++i) {
            argTypes[i] = arguments.get(i).getType().getJVMType();
        }
        return Type.getMethodDescriptor(returnType.getJVMType(), argTypes);
    }

    private final Type owner;
    private final int op;
    private final String methodName;
    private final TypeWidget returnType;
    private final BytecodeExpression target;
    private final List<BytecodeExpression> arguments;
    private final String descriptor;

    public InvokeExpression(Type owner, int op, String methodName, TypeWidget returnType, BytecodeExpression target, List<BytecodeExpression> arguments) {
        this.owner = owner;
        this.op = op;
        this.methodName = methodName;
        this.returnType = returnType;
        this.target = target;
        this.arguments = arguments;
        this.descriptor = getDescriptor(returnType, arguments);
    }

    public InvokeExpression(Class<?> owner, String methodName, TypeWidget returnType, BytecodeExpression target, List<BytecodeExpression> arguments) {
        this.owner = Type.getType(owner);
        if (owner.isInterface()) {
            this.op = Opcodes.INVOKEINTERFACE;
        } else {
            this.op = Opcodes.INVOKEVIRTUAL;
        }
        this.methodName = methodName;
        this.target = target;
        this.returnType = returnType;
        this.arguments = arguments;
        this.descriptor = getDescriptor(returnType, arguments);
    }

    public InvokeExpression(Class<?> owner, int op, String methodName, String descriptor, TypeWidget returnType, BytecodeExpression target, List<BytecodeExpression> arguments) {
        this.owner = Type.getType(owner);
        this.op = op;
        this.methodName = methodName;
        this.target = target;
        this.returnType = returnType;
        this.arguments = arguments;
        this.descriptor = descriptor;
    }

    @Override
    public TypeWidget getType() {
        return returnType;
    }

    @Override
    public void generate(CodeEmitter code) {
        if (op != Opcodes.INVOKESTATIC) {
            target.generate(code);
        }
        for (BytecodeExpression arg : arguments) {
            arg.generate(code);
        }
        MethodVisitor mv = code.getMethodVisitor();
        mv.visitMethodInsn(op, owner.getInternalName(), methodName, descriptor, op == Opcodes.INVOKEINTERFACE);
    }

}
