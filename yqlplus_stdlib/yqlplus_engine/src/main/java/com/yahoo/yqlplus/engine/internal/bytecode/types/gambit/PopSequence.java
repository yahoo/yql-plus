/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;


public class PopSequence implements BytecodeSequence {
    private final TypeWidget type;

    public PopSequence(TypeWidget type) {
        this.type = type;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.pop(type);
    }
}
