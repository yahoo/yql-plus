/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;


import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;

public interface BytecodeSequence {
    public static final BytecodeSequence NOOP = new BytecodeSequence() {
        @Override
        public void generate(CodeEmitter code) {

        }
    };

    void generate(CodeEmitter code);

}
