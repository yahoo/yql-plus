/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.internal.parser;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;

/**
 * Provides semantic helper functions to Parser.
 */
public abstract class ParserBase extends Parser {
    public ParserBase(TokenStream input) {
        super(input);
    }
}
