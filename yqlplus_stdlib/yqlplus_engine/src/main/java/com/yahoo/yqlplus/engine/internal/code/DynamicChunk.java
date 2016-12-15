/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

class DynamicChunk extends CodeChunk {
    private String indent;
    private CodeRenderable renderable;

    DynamicChunk(String indent, CodeRenderable renderable) {
        this.indent = indent;
        this.renderable = renderable;
    }

    @Override
    protected void renderChunk(final StringBuilder output) {
        renderable.render(new StringCodePrinter(indent, output));
    }
}
