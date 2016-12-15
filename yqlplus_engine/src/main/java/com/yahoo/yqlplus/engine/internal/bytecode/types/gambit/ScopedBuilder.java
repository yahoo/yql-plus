/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.compiler.LocalCodeChunk;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.language.parser.Location;


public interface ScopedBuilder extends GambitCreator {
    BytecodeExpression evaluateInto(String name, BytecodeExpression expr);

    BytecodeExpression evaluateInto(BytecodeExpression input);

    // tryCatch(E) -> Result (V | error)
    BytecodeExpression evaluateTryCatch(Location loc, BytecodeExpression expr);

    void exec(BytecodeSequence input);

    AssignableValue allocate(TypeWidget type);

    AssignableValue allocate(String name, TypeWidget type);

    AssignableValue local(BytecodeExpression input);

    AssignableValue local(String name);

    void alias(String from, String to);

    void inc(AssignableValue lv, int count);

    AssignableValue local(Location loc, String name);

    void set(Location loc, AssignableValue lv, BytecodeExpression expr);

    LocalCodeChunk getCode();

    ScopeBuilder block();

    ScopeBuilder point();
}
