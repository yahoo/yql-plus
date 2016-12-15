/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.api;

public interface InvocationResultHandler {
    void fail(Throwable t);

    void succeed(String name, Object value);

    void fail(String name, Throwable t);

    void end();
}
