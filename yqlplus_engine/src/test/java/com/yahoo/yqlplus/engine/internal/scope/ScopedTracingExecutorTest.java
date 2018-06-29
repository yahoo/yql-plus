/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

public class ScopedTracingExecutorTest {
    @Test
    public void requireScoping() {
//        ExecutorService service = Executors.newFixedThreadPool(5);
//        ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
//        ExecutionScoper scoper = new ExecutionScoper();
//        RequestMetricSink sink = new RequestMetricSink() {
//            @Override
//            public void emitRequest(RequestEvent requestEvent) {
//                // todo: verify structure
//            }
//        };
//        StandardRequestEmitter emitter = new StandardRequestEmitter(new MetricDimension(), sink);
//        TimeoutTracker tracker = new TimeoutTracker(1L, TimeUnit.SECONDS, new RelativeTicker(Ticker.systemTicker()));
//        ScopedObjects scope = new ScopedObjects(
//                new EmptyExecutionScope()
//        );
//        final ScopedTracingExecutor tracingExecutor = new ScopedTracingExecutor(
//                timers,
//                service,
//                scoper,
//                emitter,
//                tracker,
//                scope,
//                new MetricDimension().with("fancy", "1")
//        );
//        ListenableFuture<Integer> result = tracingExecutor.submit(new Callable<Integer>() {
//            @Override
//            public Integer call() throws Exception {
//                return tracingExecutor.subTasks(new MetricDimension().with("ugly", "1"))
//                        .submit(new Callable<Integer>() {
//                            @Override
//                            public Integer call() throws Exception {
//                                return 2;
//                            }
//                        }).get();
//            }
//        });
//        Assert.assertEquals(2, result.get().intValue());
//        result = tracingExecutor.submitAsync(new Callable<ListenableFuture<Integer>>() {
//            @Override
//            public ListenableFuture<Integer> call() throws Exception {
//                return tracingExecutor.subTasks(new MetricDimension().with("ugly", "2"))
//                        .submit(new Callable<Integer>() {
//                            @Override
//                            public Integer call() throws Exception {
//                                return 2;
//                            }
//                        });
//            }
//        });
//        Assert.assertEquals(2, result.get().intValue());
//        tracingExecutor.shutdown();
//        emitter.complete();
    }
}
