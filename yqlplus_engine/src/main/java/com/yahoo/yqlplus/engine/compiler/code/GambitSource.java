/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.api.types.YQLType;

import java.io.OutputStream;

public class GambitSource extends TypesHandler implements GambitScope {

    public GambitSource(ASMClassSource asm) {
        super(asm);
    }

    @Override
    public void build() throws ClassNotFoundException {
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
        return source.getGeneratedClass((UnitGenerator) target);
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

