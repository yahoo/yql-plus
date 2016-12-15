/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.yahoo.yqlplus.api.trace.Tracer;

public class CurrentThreadTracer implements Tracer {
    private final ThreadLocal<Tracer> currentTracer;

    public CurrentThreadTracer(ThreadLocal<Tracer> currentTracer) {
        this.currentTracer = currentTracer;
    }

    private Tracer getCurrentTracer() {
        Tracer out = currentTracer.get();
        if (out == null) {
            throw new IllegalStateException("No current Tracer");
        }
        return out;
    }

    @Override
    public String getGroup() {
        return getCurrentTracer().getGroup();
    }

    @Override
    public String getName() {
        return getCurrentTracer().getName();
    }

    @Override
    public Tracer start(String group, String name, Object... args) {
        return getCurrentTracer().start(group, name, args);
    }

    @Override
    public Tracer start(String group, String name) {
        return getCurrentTracer().start(group, name);
    }

    @Override
    public void error(String message) {
        getCurrentTracer().error(message);
    }

    @Override
    public void error(String message, Object arg0) {
        getCurrentTracer().error(message, arg0);
    }

    @Override
    public void error(String message, Object arg0, Object... args) {
        getCurrentTracer().error(message, arg0, args);
    }

    @Override
    public void error(Throwable t, String message) {
        getCurrentTracer().error(t, message);
    }

    @Override
    public void error(Throwable t, String message, Object arg0) {
        getCurrentTracer().error(t, message, arg0);
    }

    @Override
    public void error(Throwable t, String message, Object arg0, Object... args) {
        getCurrentTracer().error(t, message, arg0, args);
    }

    @Override
    public void error(Object message) {
        getCurrentTracer().error(message);
    }

    @Override
    public void fine(String message) {
        getCurrentTracer().fine(message);
    }

    @Override
    public void fine(String message, Object arg0, Object... args) {
        getCurrentTracer().fine(message, arg0, args);
    }

    @Override
    public void fine(String message, Object arg0) {
        getCurrentTracer().fine(message, arg0);
    }

    @Override
    public void fine(Object message) {
        getCurrentTracer().fine(message);
    }

    @Override
    public void end() {
        getCurrentTracer().end();
    }

    @Override
    public void close() {
        getCurrentTracer().close();
    }
}
