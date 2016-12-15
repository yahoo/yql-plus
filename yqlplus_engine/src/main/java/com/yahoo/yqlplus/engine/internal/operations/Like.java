/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.operations;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Like {
    private static Pattern DIRECTIVE = Pattern.compile("([%_])");

    public static Pattern compileLike(CharSequence pattern) {
        // % -> .*
        // _ -> .
        // anything else -> quote
        Matcher matcher = DIRECTIVE.matcher(pattern);
        // must use StringBuffer if we want to use matcher.appendReplacement
        StringBuffer regex = new StringBuffer(pattern.length() + 10);
        while (matcher.find()) {
            String dir = matcher.group(1);
            switch (dir) {
                case "%":
                    matcher.appendReplacement(regex, ".*");
                    break;
                case "_":
                    matcher.appendReplacement(regex, ".");
                    break;
                default:
                    matcher.appendReplacement(regex, Matcher.quoteReplacement(Pattern.quote(dir)));
            }
        }
        matcher.appendTail(regex);
        return Pattern.compile(regex.toString());
    }
}
