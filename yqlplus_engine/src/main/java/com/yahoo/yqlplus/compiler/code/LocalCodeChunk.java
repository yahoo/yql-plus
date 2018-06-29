/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.Label;

public interface LocalCodeChunk extends VariableEnvironment, BytecodeSequence {
    Label getStart();

    Label getEnd();

    /**
     * Add code to sequence; POP any values it leaves on the stack.
     */
    void execute(BytecodeSequence code);

    LocalCodeChunk block();

    LocalCodeChunk point();

    LocalCodeChunk child();

    void add(BytecodeSequence code);
}
