/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.trace;

/**
 * The payload of a TraceLogEntry with a Throwable.
 */
public final class ThrowableEvent {
    public final String message;
    public final Throwable throwable;

    public ThrowableEvent(String message, Throwable throwable) {
        this.message = message;
        this.throwable = throwable;
    }
}
