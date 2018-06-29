/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import java.io.Writer;

public interface CodePrinter {
    String EOL = "\n";

    /**
     * Print a partial line to the current code buffer.
     *
     * @param pattern
     * @param args
     */
    void print(String pattern, Object... args);

    /**
     * Print a line to the current code buffer.
     *
     * @param pattern
     * @param args
     */
    void println(String pattern, Object... args);

    Writer getWriter();

    void append(CharSequence csq);
}
