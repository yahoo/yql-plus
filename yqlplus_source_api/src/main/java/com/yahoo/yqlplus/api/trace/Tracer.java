/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.trace;

public interface Tracer extends AutoCloseable {
    String getGroup();

    String getName();

    Tracer start(String group, String name, Object... args);

    Tracer start(String group, String name);

    void error(String message);

    void error(String message, Object arg0);

    void error(String message, Object arg0, Object... args);

    void error(Throwable t, String message);

    void error(Throwable t, String message, Object arg0);

    void error(Throwable t, String message, Object arg0, Object... args);

    void error(Object message);

    void fine(String message);

    void fine(String message, Object arg0, Object... args);

    void fine(String message, Object arg0);

    void fine(Object message);

    // maybe support log(Level, ...)?

    void end();

    @Override
    void close();
}
