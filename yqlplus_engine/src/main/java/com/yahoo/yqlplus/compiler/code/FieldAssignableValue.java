/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Field;

public class FieldAssignableValue implements AssignableValue {
    private final String ownerInternalName;
    private final String name;
    private final String desc;
    private final TypeWidget type;
    private final BytecodeExpression target;

    public FieldAssignableValue(Field field, TypeWidget type, BytecodeExpression target) {
        this.ownerInternalName = Type.getInternalName(field.getDeclaringClass());
        this.name = field.getName();
        this.desc = type.getJVMType().getDescriptor();
        this.type = type;
        this.target = target;
    }

    public FieldAssignableValue(String ownerInternalName, String name, TypeWidget type, BytecodeExpression target) {
        this.ownerInternalName = ownerInternalName;
        this.name = name;
        this.type = type;
        this.desc = type.getJVMType().getDescriptor();
        this.target = target;
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    @Override
    public BytecodeExpression read() {
        return new InstanceFieldExpression(ownerInternalName, name, type, target);
    }

    @Override
    public BytecodeSequence write(final BytecodeExpression value) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                target.generate(code);
                value.generate(code);
                code.cast(type, value.getType());
                code.getMethodVisitor().visitFieldInsn(Opcodes.PUTFIELD, ownerInternalName, name, desc);
            }
        };
    }

    @Override
    public BytecodeSequence write(final TypeWidget top) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.cast(type, top);
                target.generate(code);
                code.swap(target.getType(), type);
                code.getMethodVisitor().visitFieldInsn(Opcodes.PUTFIELD, ownerInternalName, name, desc);
            }
        };
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(read());
    }
}
