/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.exprs;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Opcodes;

public class InstanceFieldExpression extends BaseTypeExpression {
    private final String ownerName;
    private final String name;
    private final BytecodeExpression target;

    public InstanceFieldExpression(String ownerName, String name, TypeWidget type, BytecodeExpression target) {
        super(type);
        this.ownerName = ownerName;
        this.name = name;
        this.target = target;
    }

    @Override
    public void generate(CodeEmitter code) {
        target.generate(code);
        code.getMethodVisitor().visitFieldInsn(Opcodes.GETFIELD, ownerName, name, getType().getJVMType().getDescriptor());
    }
}
