/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

public interface InvocableBuilder extends ScopedBuilder {
    BytecodeExpression addArgument(String name, TypeWidget type);

    Invocable complete(BytecodeExpression result);
}
