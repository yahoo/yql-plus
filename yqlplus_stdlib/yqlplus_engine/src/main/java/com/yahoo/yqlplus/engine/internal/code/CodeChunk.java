/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

abstract class CodeChunk {
    protected CodeChunk prev = null;
    protected CodeChunk next = null;

    protected abstract void renderChunk(StringBuilder output);

    private CodeChunk start() {
        CodeChunk current = this;
        while (current.prev != null) {
            current = current.prev;
        }
        return current;
    }

    private void renderChain(StringBuilder output) {
        CodeChunk current = this;
        while (current != null) {
            current.renderChunk(output);
            current = current.next;
        }
    }

    public void render(StringBuilder output) {
        start().renderChain(output);
    }

    protected <T extends CodeChunk> T insert(T chunk) {
        chunk.prev = prev;
        chunk.next = this;
        if (prev != null) {
            chunk.prev.next = chunk;
        }
        this.prev = chunk;
        return chunk;
    }
}
