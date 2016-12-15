/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.yahoo.cloud.metrics.api.Duration;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.MetricEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;

import java.util.concurrent.TimeUnit;

public class CurrentThreadTaskMetricEmitter implements TaskMetricEmitter {
    private final ThreadLocal<TaskMetricEmitter> currentTaskMetricEmitter;

    public CurrentThreadTaskMetricEmitter(ThreadLocal<TaskMetricEmitter> currentTaskMetricEmitter) {
        this.currentTaskMetricEmitter = currentTaskMetricEmitter;
    }

    @Override
    public TaskMetricEmitter start(String s, String s2) {
        return currentTaskMetricEmitter.get().start(s, s2);
    }

    @Override
    public TaskMetricEmitter start(String s, String s2, String s3, String s4) {
        return currentTaskMetricEmitter.get().start(s, s2, s3, s4);
    }

    @Override
    public TaskMetricEmitter start(String s, String s2, String s3, String s4, String s5, String s6) {
        return currentTaskMetricEmitter.get().start(s, s2, s3, s4, s5, s6);
    }

    @Override
    public TaskMetricEmitter start(String s, String s2, String s3, String s4, String s5, String s6, String... strings) {
        return currentTaskMetricEmitter.get().start(s, s2, s3, s4, s5, s6, strings);
    }

    @Override
    public TaskMetricEmitter start(MetricDimension dimension) {
        return currentTaskMetricEmitter.get().start(dimension);
    }

    @Override
    public void end() {
        currentTaskMetricEmitter.get().end();
    }

    @Override
    public MetricDimension dimensions() {
        return currentTaskMetricEmitter.get().dimensions();
    }

    @Override
    public MetricEmitter create(String s, String s2) {
        return currentTaskMetricEmitter.get().create(s, s2);
    }

    @Override
    public MetricEmitter create(String s, String s2, String s3, String s4) {
        return currentTaskMetricEmitter.get().create(s, s2, s3, s4);
    }

    @Override
    public MetricEmitter create(String s, String s2, String s3, String s4, String s5, String s6) {
        return currentTaskMetricEmitter.get().create(s, s2, s3, s4, s5, s6);
    }

    @Override
    public MetricEmitter create(String s, String s2, String s3, String s4, String s5, String s6, String... strings) {
        return currentTaskMetricEmitter.get().create(s, s2, s3, s4, s5, s6, strings);
    }

    @Override
    public MetricEmitter with(MetricDimension dimension) {
        return currentTaskMetricEmitter.get().with(dimension);
    }

    @Override
    public Duration start(String s) {
        return currentTaskMetricEmitter.get().start(s);
    }

    @Override
    public void emit(String s, long l) {
        currentTaskMetricEmitter.get().emit(s, l);
    }

    @Override
    public void emitDuration(String s, long l, TimeUnit timeUnit) {
        currentTaskMetricEmitter.get().emitDuration(s, l, timeUnit);
    }

    @Override
    public void emitSpan(String s, long l, long l2, TimeUnit timeUnit) {
        currentTaskMetricEmitter.get().emitSpan(s, l, l2, timeUnit);
    }

    @Override
    public void close() throws Exception {
        currentTaskMetricEmitter.get().close();
    }

    @Override
    public long getTaskId() {
        return currentTaskMetricEmitter.get().getTaskId();
    }
}
