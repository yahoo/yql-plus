/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.runtime;

import com.google.common.base.Ticker;
import com.yahoo.yqlplus.api.trace.TraceLogEntry;
import com.yahoo.yqlplus.api.trace.Tracer;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;

abstract class LoggingTracer implements Tracer {
    private final int id;
    private final Ticker ticker;
    private final ConcurrentLinkedQueue<TraceLogEntry> log;
    private final int logThreshold;

    protected LoggingTracer(int id, Ticker ticker, ConcurrentLinkedQueue<TraceLogEntry> log, Level logLevel) {
        this.id = id;
        this.ticker = ticker;
        this.log = log;
        this.logThreshold = logLevel.intValue();
    }

    @Override
    public final void error(String message) {
        log(Level.SEVERE, message);
    }

    @Override
    public final void error(String message, Object arg0) {
        log(Level.SEVERE, message, arg0);
    }


    @Override
    public final void error(String message, Object arg0, Object... args) {
        log(Level.SEVERE, message, arg0, args);
    }

    @Override
    public final void error(Throwable t, String message) {
        log(Level.SEVERE, t, message);
    }

    @Override
    public final void error(Throwable t, String message, Object arg0) {
        log(Level.SEVERE, message, arg0);
    }

    @Override
    public final void error(Throwable t, String message, Object arg0, Object... args) {
        log(Level.SEVERE, t, message, arg0, args);
    }

    @Override
    public final void error(Object message) {
        log(Level.SEVERE, message);
    }

    @Override
    public final void fine(String message) {
        log(Level.FINE, message);
    }

    @Override
    public final void fine(String message, Object arg0, Object... args) {
        log(Level.FINE, message, arg0, args);
    }

    @Override
    public final void fine(String message, Object arg0) {
        log(Level.FINE, message, arg0);
    }

    @Override
    public final void fine(Object message) {
        log(Level.FINE, message);
    }

    private void log(Level level, String message) {
        if (level.intValue() >= logThreshold) {
            log.add(new StringEntry(id, message, ticker.read(), level));
        }
    }

    private void log(Level level, String message, Object arg0) {
        if (level.intValue() >= logThreshold) {
            log.add(new StringEntry(id, ticker.read(), level, message, arg0));
        }
    }

    private void log(Level level, String message, Object... args) {
        if (level.intValue() >= logThreshold) {
            log.add(new StringEntry(id, ticker.read(), level, message, args));
        }
    }

    private void log(Level level, Throwable t, String message, Object... args) {
        if (level.intValue() >= logThreshold) {
            log.add(new ThrowableEntry(id, ticker.read(), level, t, message, args));
        }
    }

    private void log(Level level, Throwable t, String message) {
        if (level.intValue() >= logThreshold) {
            log.add(new ThrowableEntry(id, message, t, ticker.read(), level));
        }
    }

    private void log(Level level, Object payload) {
        if (level.intValue() >= logThreshold) {
            log.add(new PayloadEntry(id, ticker.read(), level, payload));
        }
    }

}
