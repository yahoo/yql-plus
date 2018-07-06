/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

class BoxedLDCConstant extends LDCConstant {

    BoxedLDCConstant(TypeWidget type, Object constant) {
        super(type, constant);
    }

    @Override
    public void generate(CodeEmitter environment) {
        super.generate(environment);
        environment.box(getType().unboxed());
    }
}
