/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.trace;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Timeout {
    TimeUnit getTickUnits();

    long remainingTicks();

    long getRemaining(TimeUnit units);

    boolean check();

    long verify() throws TimeoutException;

    long verify(long min, TimeUnit minUnits) throws TimeoutException;

    /**
     * Return min(maximum, remaining).
     */
    long computeMaximum(long maximum, TimeUnit maximumUnits) throws TimeoutException;

    /**
     * Compute min(maximum, remaining) and verify it is greater than minimum.
     * <p/>
     * Ensure we have at least minimum time to proceed.
     */
    long computeMaximum(long minimum, TimeUnit minimumUnits, long maximum, TimeUnit maximumUnits) throws TimeoutException;

    Timeout createTimeout(long maximum, TimeUnit maximumUnits) throws TimeoutException;

    Timeout createTimeout(Timeout maximum) throws TimeoutException;

    Timeout createTimeout(long minimum, TimeUnit minimumUnits, long maximum, TimeUnit maximumUnits) throws TimeoutException;
}
