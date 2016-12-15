/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import com.google.common.base.Preconditions;

import java.io.Writer;

public class CodeOutput extends CodeChunk implements CodeBlock {
    private final Env env;
    private final StringBuilder indent;
    private final int blockIndent;
    private StringBuilder buffer;
    private boolean begin = true;

    public String toDumpString() {
    	return CodeFormatter.toDumpString(toString());
    }

    static class Env {
        int sym = -1;

        String gensym() {
            return String.format("sym%d", ++sym);
        }
    }

    /**
     * Create a new root.
     */
    public CodeOutput() {
        this.buffer = new StringBuilder();
        this.indent = new StringBuilder();
        this.env = new Env();
        this.blockIndent = 0;
    }

    protected CodeOutput(Env env, String indent, StringBuilder buffer) {
        this.env = env;
        this.buffer = buffer;
        this.indent = new StringBuilder();
        this.indent.append(indent);
        this.blockIndent = indent.length();
    }

    @Override
    public Writer getWriter() {
        return new BufferWriter(buffer);
    }

    @Override
    protected void renderChunk(StringBuilder output) {
        output.append(buffer);
    }

    @Override
    public void indent() {
        indent.append("   ");
    }

    @Override
    public void dedent() {
        Preconditions.checkState(indent.length() > blockIndent);
        indent.setLength(indent.length() - 3);
    }

    @Override
    public void print(String pattern, Object... args) {
        if (begin) {
            buffer.append(indent);
            begin = false;
        }
        if (args != null && args.length > 0) {
            buffer.append(String.format(pattern, args));
        } else {
            buffer.append(pattern);
        }
    }


    @Override
    public void println(String pattern, Object... args) {
        print(pattern, args);
        buffer.append(EOL);
        begin = true;
    }

    @Override
    protected <T extends CodeChunk> T insert(T chunk) {
        T result = super.insert(chunk);
        this.buffer = new StringBuilder();
        return result;
    }

    @Override
    public CodeBlock block() {
        // begin a new block here.
        // pass our current buffer to the new block (so appends to that block will be inserted before future appends to this one)
        return insert(new CodeOutput(env, this.indent.toString(), this.buffer));
    }

    public CodeOutput add(CodeRenderable renderable) {
        block();
        insert(new DynamicChunk(this.indent.toString(), renderable));
        return this;

    }

    @Override
    public String gensym() {
        return env.gensym();
    }


    public String toString() {
        StringBuilder output = new StringBuilder();
        render(output);
        return output.toString();
    }

    @Override
    public void append(CharSequence csq) {
        buffer.append(csq);
    }
}
