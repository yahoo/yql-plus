/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

public class ProgramVarExpr extends LocalVarExpr {
    public ProgramVarExpr(TypeWidget type) {
        super(type, "$program");
    }
}
