/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

public class GotoSequence implements BytecodeSequence {
    private final Label label;

    public GotoSequence(Label done) {
        this.label = done;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.getMethodVisitor().visitJumpInsn(Opcodes.GOTO, label);
    }
}
