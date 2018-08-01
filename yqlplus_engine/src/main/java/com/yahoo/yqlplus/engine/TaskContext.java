/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.compiler.runtime.RelativeTicker;
import com.yahoo.yqlplus.engine.compiler.runtime.TimeoutTracker;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public final class TaskContext {
    public final TaskContext rootContext;
    public final TaskMetricEmitter metricEmitter;
    public final Tracer tracer;
    public final Timeout timeout;
    public final ForkJoinPool pool;

    public static class Builder {
        TaskMetricEmitter metricEmitter = DummyMetricEmitter.instance;
        Tracer tracer = DummyTracer.instance;
        Timeout timeout;
        ForkJoinPool pool = ForkJoinPool.commonPool();

        private Builder() {

        }

        public Builder withEmitter(TaskMetricEmitter emitter) {
            this.metricEmitter = emitter;
            return this;
        }

        public Builder withTracer(Tracer tracer) {
            this.tracer = tracer;
            return this;
        }

        public Builder withTimeout(Timeout timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withTimeout(long timeout, TimeUnit timeoutUnits) {
            this.timeout = new TimeoutTracker(timeout, timeoutUnits, new RelativeTicker(Ticker.systemTicker()));
            return this;
        }

        public Builder withPool(ForkJoinPool pool) {
            this.pool = pool;
            return this;
        }

        public TaskContext build() {
            if (timeout == null) {
                withTimeout(30L, TimeUnit.SECONDS);
            }
            return new TaskContext(metricEmitter, tracer, timeout, pool);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private TaskContext(TaskMetricEmitter metricEmitter, Tracer tracer, Timeout timeout, ForkJoinPool pool) {
        this.rootContext = this;
        this.metricEmitter = metricEmitter;
        this.tracer = tracer;
        this.timeout = timeout;
        this.pool = pool;
    }

    private TaskContext(TaskContext rootContext, TaskMetricEmitter metricEmitter, Tracer tracer, Timeout timeout, ForkJoinPool pool) {
        this.rootContext = rootContext;
        this.metricEmitter = metricEmitter;
        this.tracer = tracer;
        this.timeout = timeout;
        this.pool = pool;
    }

    public final <T> T runTimeout(Supplier<T> work) throws ExecutionException, InterruptedException {
        CompletableFuture<T> f = CompletableFuture.supplyAsync(work, pool);
        f.orTimeout(timeout.remainingTicks(), timeout.getTickUnits());
        return f.get();
    }

    public TaskContext start(MetricDimension ctx) {
        return new TaskContext(rootContext, metricEmitter.start(ctx), tracer.start(ctx.getKey(), ctx.getValue()), timeout, pool);
    }

    public TaskContext timeout(long timeout, TimeUnit units) throws TimeoutException {
        return new TaskContext(rootContext, metricEmitter, tracer, this.timeout.createTimeout(timeout, units), pool);
    }

    public TaskContext timeout(long min, TimeUnit minUnits, long max, TimeUnit maxUnits) throws TimeoutException {
        return new TaskContext(rootContext, metricEmitter, tracer, this.timeout.createTimeout(min, minUnits, max, maxUnits), pool);
    }

    public <T> List<T> scatter(List<Supplier<T>> input) throws ExecutionException, InterruptedException {
        CompletableFuture[] futures = new CompletableFuture[input.size()];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = CompletableFuture.supplyAsync(input.get(i), pool);
        }
        CompletableFuture<Void> done = CompletableFuture.allOf(futures);
        done.orTimeout(timeout.remainingTicks(), timeout.getTickUnits());
        done.get();
        List<T> result = Lists.newArrayListWithExpectedSize(input.size());
        for (int i = 0; i < futures.length; i++) {
            result.add((T) futures[i].get());
        }
        return result;
    }

    public void execute(Runnable runnable) {
        pool.submit(runnable);
    }


    public void executeAll(Runnable one) {
        one.run();
    }

    public void executeAll(Runnable... runnables) {
        for (int i = 0; i < runnables.length - 1; ++i) {
            execute(runnables[i]);
        }
        runnables[runnables.length - 1].run();
    }


    public void end() {
        metricEmitter.end();
        tracer.end();
    }
}
