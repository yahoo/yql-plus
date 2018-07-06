/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.List;

public class InvokeDynamicExpression implements BytecodeExpression {
    private final Handle bootstrap;
    private final String operationName;
    private final TypeWidget returnType;
    private final List<BytecodeExpression> arguments;
    private final Object[] bootstrapArgs;

    public InvokeDynamicExpression(Handle bootstrap, String operationName, TypeWidget expectedReturnType, List<BytecodeExpression> arguments, Object... bootstrapArgs) {
        this.bootstrap = bootstrap;
        this.operationName = operationName;
        this.returnType = expectedReturnType;
        this.arguments = arguments;
        this.bootstrapArgs = bootstrapArgs;
    }

    @Override
    public TypeWidget getType() {
        return returnType;
    }

    @Override
    public void generate(CodeEmitter code) {
        for (BytecodeExpression arg : arguments) {
            arg.generate(code);
        }
        Type[] argTypes = new Type[arguments.size()];
        for (int i = 0; i < arguments.size(); ++i) {
            argTypes[i] = arguments.get(i).getType().getJVMType();
        }
        String desc = Type.getMethodDescriptor(returnType.getJVMType(), argTypes);
        MethodVisitor mv = code.getMethodVisitor();
        mv.visitInvokeDynamicInsn(operationName, desc, bootstrap, bootstrapArgs);
    }
}
