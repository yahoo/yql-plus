/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

import java.io.IOException;
import java.io.OutputStream;

public class GambitSource extends TypesHandler implements GambitScope {

    public GambitSource(ASMClassSource asm) {
        super(asm);
    }

    @Override
    public void build() throws IOException, ClassNotFoundException {
        source.build();
    }

    public void dump(OutputStream out) {
        source.dump(out);
    }

    public void trace(OutputStream out) {
        source.trace(out);
    }

    @Override
    public Class<?> getObjectClass(ObjectBuilder target) throws ClassNotFoundException {
        return source.getGeneratedClass((com.yahoo.yqlplus.engine.internal.bytecode.UnitGenerator) target);
    }

    @Override
    public YQLType createYQLType(TypeWidget type) {
        return source.createYQLType(type);
    }

    @Override
    public void addClass(Class<?> clazz) {
        source.getType(clazz);
    }
}

