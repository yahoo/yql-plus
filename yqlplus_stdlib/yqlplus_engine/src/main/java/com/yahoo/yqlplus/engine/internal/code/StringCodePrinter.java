/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import java.io.Writer;

class StringCodePrinter implements CodePrinter {
    private final StringBuilder output;
    private final String indent;
    boolean begin;

    public StringCodePrinter(String indent, StringBuilder output) {
        this.output = output;
        this.indent = indent;
        begin = true;
    }

    @Override
    public void print(String pattern, Object... args) {
        if (begin) {
            output.append(indent);
            begin = false;
        }
        output.append((args != null && args.length > 0) ? String.format(pattern, args) : pattern);

    }

    @Override
    public void println(String pattern, Object... args) {
        print(pattern, args);
        output.append(EOL);
        begin = true;
    }

    @Override
    public Writer getWriter() {
        return new BufferWriter(output);
    }

    @Override
    public void append(CharSequence csq) {
        output.append(csq);
    }
}
