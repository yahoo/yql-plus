/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import java.io.IOException;
import java.io.Writer;

class BufferWriter extends Writer {
    private StringBuilder buffer;

    BufferWriter(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        buffer.append(cbuf, off, len);
    }

    @Override
    public void flush() throws IOException {
        // no-op
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
