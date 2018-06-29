/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import org.objectweb.asm.Label;

public interface IterateAdapter {
    TypeWidget getValue();

    BytecodeSequence iterate(BytecodeExpression target, IterateLoop loop);

    BytecodeSequence iterate(BytecodeExpression target, AssignableValue item, IterateLoop loop);

    BytecodeExpression first(BytecodeExpression target);

    interface IterateLoop {
        void item(CodeEmitter code, BytecodeExpression item, Label abortLoop, Label nextItem);
    }
}
