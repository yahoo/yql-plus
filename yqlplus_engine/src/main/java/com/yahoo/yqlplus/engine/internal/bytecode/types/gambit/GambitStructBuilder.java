/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.generate.StructGenerator;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class GambitStructBuilder implements StructBuilder {
    private final StructGenerator struct;

    public GambitStructBuilder(ASMClassSource source) {
        struct = new StructGenerator("struct_" + source.generateUniqueElement(), source);
    }

    @Override
    public StructBuilder add(String fieldName, TypeWidget type) {
        struct.addField(fieldName, type);
        return this;
    }

    @Override
    public TypeWidget build() {
        return struct.build();
    }
}
