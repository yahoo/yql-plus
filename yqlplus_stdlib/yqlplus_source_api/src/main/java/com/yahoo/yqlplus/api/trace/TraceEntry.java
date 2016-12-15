/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.trace;

import java.util.concurrent.TimeUnit;

public interface TraceEntry {
    int getId();

    int getParentId();

    TimeUnit getTickUnits();

    long getStartTicks();

    float getStartMilliseconds();

    long getEndTicks();

    float getEndMilliseconds();

    long getDurationTicks();

    float getDurationMilliseconds();

    String getGroup();

    String getName();
}
