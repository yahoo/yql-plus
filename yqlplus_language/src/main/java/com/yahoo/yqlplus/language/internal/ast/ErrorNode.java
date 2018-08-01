/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.internal.ast;

import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;

/**
 * Started from copy of CommonErrorNode from ANTLR3
 */
public class ErrorNode extends ParserRuleContext {
    public IntStream input;
    public Token start;
    public Token stop;
    public RecognitionException trappedException;

    public ErrorNode(TokenStream input, Token start, Token stop,
                     RecognitionException e) {
        //System.out.println("start: "+start+", stop: "+stop);
        if (stop == null ||
                (stop.getTokenIndex() < start.getTokenIndex() &&
                        stop.getType() != Token.EOF)) {
            // sometimes resync does not consume a token (when LT(1) is
            // in follow set.  So, stop will be 1 to left to start. adjust.
            // Also handle case where start is the first token and no token
            // is consumed during recovery; LT(-1) will return null.
            stop = start;
        }
        this.input = input;
        this.start = start;
        this.stop = stop;
        this.trappedException = e;
    }

    public boolean isNil() {
        return false;
    }

    public int getType() {
        return Token.INVALID_TYPE;
    }

    public String getText() {
        String badText;
        if (start != null) {
            int i = start.getTokenIndex();
            int j = stop.getTokenIndex();
            if (stop.getType() == Token.EOF) {
                j = input.size();
            }
            badText = ((TokenStream) input).getText(start, stop);
        } else {
            // people should subclass if they alter the tree type so this
            // next one is for sure correct.
            badText = "<unknown>";
        }
        return badText;
    }
}
