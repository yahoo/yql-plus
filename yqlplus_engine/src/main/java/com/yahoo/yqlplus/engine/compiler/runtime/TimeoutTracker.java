/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.api.trace.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeoutTracker implements Timeout {
    private final long timeoutTicks;
    private final long timeout;
    private final TimeUnit timeoutUnits;
    private final RelativeTicker relativeTicker;

    public TimeoutTracker(long timeout, TimeUnit timeoutUnits, RelativeTicker relativeTicker) {
        this.timeoutTicks = timeoutUnits.toNanos(timeout);
        this.timeout = timeout;
        this.timeoutUnits = timeoutUnits;
        this.relativeTicker = relativeTicker;
    }

    private TimeoutTracker(long ticks, TimeUnit reportingUnits, TimeoutTracker parent) {
        this.timeoutTicks = ticks;
        this.timeout = reportingUnits.convert(ticks, getTickUnits());
        this.timeoutUnits = reportingUnits;
        this.relativeTicker = parent.relativeTicker.child();
    }

    @Override
    public TimeUnit getTickUnits() {
        return TimeUnit.NANOSECONDS;
    }

    private long elapsedTicks() {
        return relativeTicker.read();
    }

    @Override
    public long remainingTicks() {
        return timeoutTicks - elapsedTicks();
    }

    @Override
    public long getRemaining(TimeUnit units) {
        return units.convert(remainingTicks(), getTickUnits());
    }

    @Override
    public boolean check() {
        return remainingTicks() > 0L;
    }

    @Override
    public long computeMaximum(long minimum, TimeUnit minimumUnits, long maximum, TimeUnit maximumUnits) throws TimeoutException {
        long maxTicks = maximumUnits.toNanos(maximum);
        long remainingTicks = remainingTicks();
        long minTicks = minimumUnits.toNanos(minimum);
        Preconditions.checkArgument(maxTicks >= minTicks, "Minimum (%s %s) MUST be <= maximum (%s %s)", minimum, minimumUnits, maximum, maximumUnits);
        if (remainingTicks < minTicks) {
            throw new TimeoutException(String.format("Less than minimum ticks remaining (%d %s).", minimum, minimumUnits));
        }
        return remainingTicks > maxTicks ? maxTicks : remainingTicks;
    }

    @Override
    public long computeMaximum(long maximum, TimeUnit maximumUnits) {
        long maxNanos = maximumUnits.toNanos(maximum);
        return Math.min(remainingTicks(), maxNanos);
    }

    @Override
    public long verify() throws TimeoutException {
        long rem = remainingTicks();
        if (rem <= 0L) {
            throw new TimeoutException(String.format("Timeout after %d %s.", timeout, timeoutUnits));
        }
        return rem;
    }

    @Override
    public long verify(long minimum, TimeUnit minimumUnits) throws TimeoutException {
        long remainingTicks = remainingTicks();
        long minTicks = minimumUnits.toNanos(minimum);
        if (remainingTicks < minTicks) {
            throw new TimeoutException(String.format("Less than minimum ticks remaining (%d %s).", minimum, minimumUnits));
        }
        return remainingTicks;
    }

    @Override
    public Timeout createTimeout(long maximum, TimeUnit maximumUnits) throws TimeoutException {
        long ticks = computeMaximum(maximum, maximumUnits);
        return new TimeoutTracker(ticks, maximumUnits, this);
    }

    @Override
    public Timeout createTimeout(Timeout maximum) throws TimeoutException {
        return createTimeout(maximum.remainingTicks(), maximum.getTickUnits());
    }

    @Override
    public Timeout createTimeout(long minimum, TimeUnit minimumUnits, long maximum, TimeUnit maximumUnits) throws TimeoutException {
        long ticks = computeMaximum(minimum, minimumUnits, maximum, maximumUnits);
        return new TimeoutTracker(ticks, maximumUnits, this);
    }
}
