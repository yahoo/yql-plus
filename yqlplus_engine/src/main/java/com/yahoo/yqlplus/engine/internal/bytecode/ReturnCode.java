/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Opcodes;

public class ReturnCode implements BytecodeSequence {
    private final int op;

    public ReturnCode(TypeWidget type) {
        this.op = type.getJVMType().getOpcode(Opcodes.IRETURN);
    }

    public ReturnCode(int op) {
        this.op = op;
    }

    public ReturnCode() {
        this.op = Opcodes.RETURN;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.getMethodVisitor().visitInsn(op);
    }
}
