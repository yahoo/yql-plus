/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.trace;

import java.util.List;

public interface TraceRequest {
    /**
     * Milliseconds from epoch.
     */
    long getStartTime();

    List<? extends TraceEntry> getEntries();

    List<? extends TraceLogEntry> getLog();
}
