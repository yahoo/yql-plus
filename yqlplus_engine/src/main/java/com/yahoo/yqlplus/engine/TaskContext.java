/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.common.collect.Lists;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.internal.scope.ExecutionScoper;
import com.yahoo.yqlplus.engine.internal.scope.ScopedObjects;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public final class TaskContext {
    public final TaskMetricEmitter metricEmitter;
    public final Tracer tracer;
    public final Timeout timeout;
    public final ForkJoinPool pool;
    public final ScopedObjects scope;
    public final ExecutionScoper scoper;

    public TaskContext(TaskMetricEmitter metricEmitter, Tracer tracer, Timeout timeout, ExecutionScoper scoper, ScopedObjects scope) {
        this.metricEmitter = metricEmitter;
        this.tracer = tracer;
        this.timeout = timeout;
        // TODO: give container control over this
        this.pool = ForkJoinPool.commonPool();
        this.scope = scope;
        this.scoper = scoper;
    }

    public final <T> Supplier<T> scopeSupplier(Supplier<T> work) {
        return () -> {
            try {
                scoper.enter(scope);
                return work.get();
            } finally {
                scoper.exit();
            }
        };
    }

    public final <T> T runTimeout(Supplier<T> work) throws ExecutionException, InterruptedException {
        CompletableFuture<T> f = CompletableFuture.supplyAsync(scopeSupplier(work), pool);
        f.orTimeout(timeout.remainingTicks(), timeout.getTickUnits());
        return f.get();
    }

    public TaskContext start(MetricDimension ctx) {
        return new TaskContext(metricEmitter.start(ctx), tracer.start(ctx.getKey(), ctx.getValue()), timeout, scoper, scope);
    }

    public TaskContext timeout(long timeout, TimeUnit units) throws TimeoutException {
        return new TaskContext(metricEmitter, tracer, this.timeout.createTimeout(timeout, units), scoper, scope);
    }

    public TaskContext timeout(long min, TimeUnit minUnits, long max, TimeUnit maxUnits) throws TimeoutException {
        return new TaskContext(metricEmitter, tracer, this.timeout.createTimeout(min, minUnits, max, maxUnits), scoper, scope);
    }

    public <T> List<T> scatter(List<Supplier<T>> input) throws ExecutionException, InterruptedException {
        CompletableFuture[] futures = new CompletableFuture[input.size()];
        for(int i = 0; i < futures.length; i++) {
            futures[i] = CompletableFuture.supplyAsync(scopeSupplier(input.get(i)), pool);
        }
        CompletableFuture<Void> done = CompletableFuture.allOf(futures);
        done.orTimeout(timeout.remainingTicks(), timeout.getTickUnits());
        done.get();
        List<T> result = Lists.newArrayListWithExpectedSize(input.size());
        for(int i = 0; i < futures.length; i++) {
            result.add((T) futures[i].get());
        }
        return result;
    }

    public void end() {
        metricEmitter.end();
        tracer.end();
    }
}
