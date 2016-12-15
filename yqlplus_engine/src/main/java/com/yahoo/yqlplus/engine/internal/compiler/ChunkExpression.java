/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;

public class ChunkExpression extends BaseTypeExpression {
    private LocalCodeChunk chunk;

    public ChunkExpression(TypeWidget type, LocalCodeChunk chunk) {
        super(type);
        this.chunk = chunk;
    }

    @Override
    public void generate(CodeEmitter code) {
        chunk.generate(code);
    }
}
