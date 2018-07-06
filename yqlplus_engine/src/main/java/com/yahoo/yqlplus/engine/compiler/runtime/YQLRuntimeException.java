/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;


public class YQLRuntimeException extends RuntimeException {
    private final YQLError error;

    public YQLRuntimeException(YQLError error) {
        super(error.getMessage());
        this.error = error;
    }

    @Override
    public synchronized Throwable getCause() {
        return error.getCause();
    }

    public YQLError getError() {
        return error;
    }
}
