/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class MethodAssignableValue implements AssignableValue {
    private final TypeWidget type;
    private final BytecodeExpression target;
    private final String ownerName;
    private final String name;
    private final int op;
    private final String desc;

    public MethodAssignableValue(TypeWidget type, Class ownerClass, String name, BytecodeExpression target) {
        this.type = type;
        this.target = target;
        this.ownerName = Type.getInternalName(ownerClass);
        this.name = name;
        this.op = ownerClass.isInterface() ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL;
        this.desc = Type.getMethodDescriptor(type.getJVMType());
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    @Override
    public BytecodeExpression read() {
        return new BaseTypeExpression(type) {
            @Override
            public void generate(CodeEmitter code) {
                target.generate(code);
                code.getMethodVisitor().visitMethodInsn(op, ownerName, name, desc, op == Opcodes.INVOKEINTERFACE);
            }
        };
    }

    @Override
    public BytecodeSequence write(final BytecodeExpression value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeSequence write(final TypeWidget top) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }
}
