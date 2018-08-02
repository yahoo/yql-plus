package com.yahoo.yqlplus.engine;

import com.yahoo.cloud.metrics.api.Duration;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;

import java.util.concurrent.TimeUnit;

public class DummyMetricEmitter implements TaskMetricEmitter {
    public static final TaskMetricEmitter instance = new DummyMetricEmitter();

    private final Duration duration = new Duration(0L, this, "dummyDuration");


    private DummyMetricEmitter() {
    }

    public long getTaskId() {
        return 0L;
    }

    public void emit(String name, long count) {
    }

    public void emitDuration(String name, long duration, TimeUnit unit) {
    }

    public void emitSpan(String name, long start, long end, TimeUnit unit) {
    }

    public void end() {
    }

    public MetricDimension dimensions() {
        return null;
    }

    public TaskMetricEmitter create(String key, String value) {
        return this;
    }

    public TaskMetricEmitter create(String k1, String v1, String k2, String v2) {
        return this;
    }

    public TaskMetricEmitter create(String k1, String v1, String k2, String v2, String k3, String v3) {
        return this;
    }

    public TaskMetricEmitter create(String k1, String v1, String k2, String v2, String k3, String v3, String... keyValues) {
        return this;
    }

    public TaskMetricEmitter with(MetricDimension addedDimensions) {
        return this;
    }

    public Duration start(String name) {
        return this.duration;
    }

    public void close() throws Exception {
    }

    public TaskMetricEmitter start(String key, String value) {
        return this;
    }

    public TaskMetricEmitter start(String k1, String v1, String k2, String v2) {
        return this;
    }

    public TaskMetricEmitter start(String k1, String v1, String k2, String v2, String k3, String v3) {
        return this;
    }

    public TaskMetricEmitter start(String k1, String v1, String k2, String v2, String k3, String v3, String... keyValues) {
        return this;
    }

    public TaskMetricEmitter start(MetricDimension addedDimensions) {
        return this;
    }
}