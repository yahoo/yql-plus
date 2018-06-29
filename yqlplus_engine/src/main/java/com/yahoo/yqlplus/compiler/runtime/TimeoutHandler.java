/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.runtime;

import com.google.common.util.concurrent.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.engine.internal.scope.ExecutionScoper;

import java.lang.ref.WeakReference;
import java.util.concurrent.*;

/**
 * Allocate a Timeout Budget.
 */
public final class TimeoutHandler {
    private ScheduledExecutorService timers;
    private ExecutionScoper scoper;

    @Inject
    TimeoutHandler(@Named("timeout") ScheduledExecutorService timers, ExecutionScoper scoper) {
        this.timers = timers;
        this.scoper = scoper;
    }

    public <T> ListenableFuture<T> withTimeoutSync(final Callable<T> callable, final Timeout tracker, ListeningExecutorService executor) {
        try {
            final long remaining = tracker.verify();
            final TimeUnit units = tracker.getTickUnits();
            final ListenableFuture<T> source = executor.submit(scoper.continueScope(callable));
            return withTimeout(source, remaining, units);
        } catch (TimeoutException e) {
            return Futures.immediateFailedFuture(e);
        }
    }


    public <T> ListenableFuture<T> withTimeoutAsync(final Callable<ListenableFuture<T>> callable, final Timeout tracker, ListeningExecutorService executor) {
        final SettableFuture<T> result = SettableFuture.create();
        final long remaining = tracker.remainingTicks();
        final TimeUnit units = tracker.getTickUnits();
        final ListenableFuture<ListenableFuture<T>> source = executor.submit(callable);
        final ScheduledFuture<?> scheduledFuture = timers.schedule(new IntermediateTask<>(result, remaining, units), remaining, units);
        Futures.addCallback(source, scoper.continueScope(new FutureCallback<ListenableFuture<T>>() {
            @Override
            public void onSuccess(ListenableFuture<T> next) {
                scheduledFuture.cancel(false);
                try {
                    long remaining = tracker.verify();
                    final ScheduledFuture<?> remainingFuture = timers.schedule(new TimeoutTask<>(next, result, remaining, units), remaining, units);
                    Futures.addCallback(next, scoper.continueScope(new FutureCallback<T>() {
                        @Override
                        public void onSuccess(T out) {
                            remainingFuture.cancel(false);
                            result.set(out);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            remainingFuture.cancel(false);
                            result.setException(t);
                        }
                    }), MoreExecutors.directExecutor());
                } catch (TimeoutException e) {
                    next.cancel(true);
                    result.setException(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                scheduledFuture.cancel(false);
                result.setException(t);
            }
        }), MoreExecutors.directExecutor());
        return scoper.scopeCallbacks(result);
    }

    public <T> ListenableFuture<T> withTimeout(final ListenableFuture<T> source, Timeout timeout) {
        return withTimeout(source, timeout.remainingTicks(), timeout.getTickUnits());
    }


    public <T> ListenableFuture<T> withTimeout(final ListenableFuture<T> source, long timeout, TimeUnit timeoutUnits) {
        if (timeout != 0) {
            final SettableFuture<T> result = SettableFuture.create();
            final Future<?> scheduledFuture = timers.schedule(new TimeoutTask<T>(source, result, timeout, timeoutUnits), timeout, timeoutUnits);
            result.addListener(new Runnable() {
                @Override
                public void run() {
                    scheduledFuture.cancel(false);
                    if (result.isCancelled()) {
                        source.cancel(true);
                    }
                }
            }, MoreExecutors.directExecutor());
            Futures.addCallback(source, new FutureCallback<T>() {
                @Override
                public void onSuccess(T out) {
                    scheduledFuture.cancel(false);
                    result.set(out);
                }

                @Override
                public void onFailure(Throwable t) {
                    scheduledFuture.cancel(false);
                    result.setException(t);
                }
            }, MoreExecutors.directExecutor());
            return scoper.scopeCallbacks(result);
        } else {
            return source;
        }
    }

    private static final class TimeoutTask<V> implements Runnable {
        private final WeakReference<ListenableFuture<V>> requestFuture;
        private final WeakReference<SettableFuture<V>> resultFuture;
        private final long timeout;
        private final TimeUnit unit;

        public TimeoutTask(ListenableFuture<V> requestFuture, SettableFuture<V> resultFuture, long timeout, TimeUnit unit) {
            this.requestFuture = new WeakReference<>(requestFuture);
            this.resultFuture = new WeakReference<>(resultFuture);
            this.timeout = timeout;
            this.unit = unit;
        }

        @Override
        public void run() {
            ListenableFuture<V> reqFuture = requestFuture.get();
            SettableFuture<V> resFuture = resultFuture.get();

            if (reqFuture == null || resFuture == null) {
                return;
            }
            resFuture.setException(new TimeoutException(String.format("Timeout after %d %s.", timeout, unit)));
            reqFuture.cancel(true);
        }
    }

    private static final class IntermediateTask<V> implements Runnable {
        private final WeakReference<SettableFuture<V>> resultFuture;
        private final long timeout;
        private final TimeUnit unit;

        public IntermediateTask(SettableFuture<V> resultFuture, long timeout, TimeUnit unit) {
            this.resultFuture = new WeakReference<>(resultFuture);
            this.timeout = timeout;
            this.unit = unit;
        }

        @Override
        public void run() {
            SettableFuture<V> resFuture = resultFuture.get();

            if (resFuture == null) {
                return;
            }
            resFuture.setException(new TimeoutException(String.format("Timeout after %d %s.", timeout, unit)));
        }
    }
}
