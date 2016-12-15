/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.trace;

import java.util.logging.Level;

public interface TraceLogEntry {
    /**
     * The Tracer which emitted this entry.
     */
    int getTraceId();

    /**
     * Logging level.
     */
    Level getLevel();

    /**
     * Ticks since start of request (in TraceEntry.getTickUnits())
     */
    long getTicks();

    /**
     * Logging entry (may be a String or a structured type).
     */
    Object get();

    /**
     * TraceLogEntry will implement a toString which produces a human-readable value (assuming the payload toString also does)
     */
    String toString();
}
