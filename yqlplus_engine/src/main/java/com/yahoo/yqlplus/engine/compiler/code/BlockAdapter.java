/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public class BlockAdapter extends ExpressionHandler implements GambitCreator.ScopeBuilder {
    public BlockAdapter(ASMClassSource source, LocalCodeChunk body) {
        super(source);
        this.body = body;
    }

    @Override
    public BytecodeExpression complete(BytecodeExpression end) {
        body.add(end);
        return new BaseTypeExpression(end.getType()) {
            @Override
            public void generate(CodeEmitter code) {
                body.generate(code);
            }
        };
    }

}
