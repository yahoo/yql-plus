/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.api;

import java.io.OutputStream;

public interface NativeInvocationResultHandler {
    void fail(Throwable t);

    OutputStream createStream(String name);

    void succeed(String name);

    void fail(String name, Throwable t);

    void end();
}
