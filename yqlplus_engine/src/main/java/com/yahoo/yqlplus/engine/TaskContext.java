/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.trace.Tracer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TaskContext {
    public final TaskMetricEmitter metricEmitter;
    public final Tracer tracer;
    public final Timeout timeout;

    public TaskContext(TaskMetricEmitter metricEmitter, Tracer tracer, Timeout timeout) {
        this.metricEmitter = metricEmitter;
        this.tracer = tracer;
        this.timeout = timeout;
    }

    public TaskContext start(MetricDimension ctx) {
        return new TaskContext(metricEmitter.start(ctx), tracer.start(ctx.getKey(), ctx.getValue()), timeout);
    }

    public TaskContext timeout(long timeout, TimeUnit units) throws TimeoutException {
        return new TaskContext(metricEmitter, tracer, this.timeout.createTimeout(timeout, units));
    }

    public TaskContext timeout(long min, TimeUnit minUnits, long max, TimeUnit maxUnits) throws TimeoutException {
        return new TaskContext(metricEmitter, tracer, this.timeout.createTimeout(min, minUnits, max, maxUnits));
    }

    public void end() {
        metricEmitter.end();
        tracer.end();
    }
}
