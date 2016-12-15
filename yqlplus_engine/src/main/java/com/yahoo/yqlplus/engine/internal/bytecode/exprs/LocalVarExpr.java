/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.exprs;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.LocalValueExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class LocalVarExpr extends LocalValueExpression {
    private String name;

    public LocalVarExpr(TypeWidget type, String name) {
        super(type);
        this.name = name;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.getLocal(name).read().generate(code);
    }
}
