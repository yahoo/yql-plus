/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import org.objectweb.asm.Label;

public interface ComparisonAdapter {
    void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull);
}
