/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public class YQLTypeException extends RuntimeException {
    public YQLTypeException() {
    }

    public static void notComparable(Object target, int line, int offset) {
        throw new YQLTypeException("L" + line + ":" + offset + ": unable to compare " + target);
    }


    public YQLTypeException(String message) {
        super(message);
    }

    public YQLTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public YQLTypeException(Throwable cause) {
        super(cause);
    }

    public YQLTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
