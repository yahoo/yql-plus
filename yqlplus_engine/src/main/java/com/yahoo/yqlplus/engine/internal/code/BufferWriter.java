/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import java.io.Writer;

class BufferWriter extends Writer {
    private StringBuilder buffer;

    BufferWriter(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public void write(char[] cbuf, int off, int len) {
        buffer.append(cbuf, off, len);
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
