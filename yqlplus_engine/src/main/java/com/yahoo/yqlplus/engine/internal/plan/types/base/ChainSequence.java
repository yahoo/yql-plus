/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;

import java.util.Arrays;

public class ChainSequence implements BytecodeSequence {
    private Iterable<BytecodeSequence> seq;

    public ChainSequence(BytecodeSequence... seq) {
        this.seq = Arrays.asList(seq);
    }

    @Override
    public void generate(CodeEmitter code) {
        for(BytecodeSequence s : seq) {
            code.exec(s);
        }
    }
}
