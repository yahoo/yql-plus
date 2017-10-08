/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.google.common.util.concurrent.*;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;
import com.yahoo.yqlplus.engine.scope.WrapScope;

import javax.annotation.Nullable;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class ScopedTracingExecutor extends AbstractExecutorService implements ListeningExecutorService {
    private final ScheduledExecutorService timers;
    private final ExecutorService work;
    private final ExecutionScoper scoper;
    private final ThreadLocal<TaskMetricEmitter> currentTask;
    private final ThreadLocal<Tracer> currentTracer;
    private final TaskMetricEmitter task;
    private final Timeout timeout;
    private final ScopedObjects scope;
    private final TaskContext rootContext;
    private final AtomicInteger threadIdSource;

    public ScopedTracingExecutor(ScheduledExecutorService timers, ExecutorService work, ExecutionScoper scoper, TaskMetricEmitter task, Tracer tracer, Timeout timeout, ExecutionScope scope) {
        this.rootContext = new TaskContext(task, tracer, timeout);
        this.timers = timers;
        this.work = work;
        this.scoper = scoper;
        this.task = task;
        this.timeout = timeout;
        this.currentTask = new ThreadLocal<>();
        this.currentTracer = new ThreadLocal<>();
        this.scope = new ScopedObjects(new WrapScope(scope)
                .bind(Key.get(ListeningExecutorService.class, Names.named("programExecutor")), this)
                .bind(Key.get(ScheduledExecutorService.class, Names.named("programTimeout")), timers)
                .bind(Key.get(TaskMetricEmitter.class, Names.named("task")), new CurrentThreadTaskMetricEmitter(currentTask))
                .bind(Tracer.class, new CurrentThreadTracer(currentTracer))
                .bind(Key.get(TaskContext.class, Names.named("rootContext")), new TaskContext(task, tracer, timeout)));
        this.threadIdSource = new AtomicInteger(0);
    }

    protected ScopedTracingExecutor(ScheduledExecutorService timers, ExecutorService work, ExecutionScoper scoper, ThreadLocal<TaskMetricEmitter> currentTask, ThreadLocal<Tracer> currentTracer, TaskMetricEmitter task, Timeout timeout, ScopedObjects scope, TaskContext rootContext, AtomicInteger threadIdSource) {
        this.currentTracer = currentTracer;
        this.currentTask = currentTask;
        this.timers = timers;
        this.work = work;
        this.scoper = scoper;
        this.task = task;
        this.timeout = timeout;
        this.scope = scope;
        this.rootContext = rootContext;
        this.threadIdSource = threadIdSource;
    }

    public ScopedTracingExecutor createSubExecutor(TaskContext context) {
        TaskMetricEmitter currentTask = this.currentTask.get();
        if (currentTask == null) {
            currentTask = task;
        }
        return new ScopedTracingExecutor(timers, work, scoper, this.currentTask, this.currentTracer, currentTask, context.timeout, scope, context, threadIdSource);
    }

    @Override
    public void shutdown() {
        work.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return work.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return work.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return work.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return work.awaitTermination(timeout, unit);
    }

    @Override
    protected final <T> ListenableFutureTask<T> newTaskFor(Runnable runnable, T value) {
        return ListenableFutureTask.create(runnable, value);
    }

    @Override
    protected final <T> ListenableFutureTask<T> newTaskFor(Callable<T> callable) {
        return ListenableFutureTask.create(callable);
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
        return wrapResult((ListenableFuture<?>) super.submit(task));
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, @Nullable T result) {
        return wrapResult((ListenableFuture<T>) super.submit(task, result));
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
        return wrapResult((ListenableFuture<T>) super.submit(task));
    }

    public <T> ListenableFuture<T> submitAsync(Callable<ListenableFuture<T>> task) {
        return withTimeoutAsync(task, timeout);
    }

    public Runnable wrap(final Runnable command) {
        return new Runnable() {
            @Override
            public void run() {
                final Thread thread = Thread.currentThread();
                final String name = thread.getName();
                try {
                    scoper.enter(scope);
                    currentTask.set(task.start(new MetricDimension()));
                    currentTracer.set(rootContext.tracer.start("thread", name));
                    thread.setName(name + ": " + currentTask.get().dimensions());
                    if (timeout.check()) {
                        command.run();
                    } else {
                        currentTask.get().emit("taskTimeout", 1);
                    }
                } finally {
                    scoper.exit();
                    TaskMetricEmitter emitter = currentTask.get();
                    if (emitter != null) {
                        emitter.end();
                    }
                    currentTask.remove();
                    Tracer tracer = currentTracer.get();
                    if (tracer != null) {
                        tracer.end();
                    }
                    currentTracer.remove();
                    thread.setName(name);
                }
            }
        };
    }

    @Override
    public void execute(final Runnable command) {
        work.execute(wrap(command));
    }

    public void runNow(Runnable command) {
        wrap(command).run();
    }

    private <T> ListenableFuture<T> wrapResult(ListenableFuture<T> input) {
        return withTimeout(input, timeout);
    }


    public <T> ListenableFuture<T> withTimeoutAsync(final Callable<ListenableFuture<T>> callable, final Timeout tracker) {
        final SettableFuture<T> result = SettableFuture.create();
        final TimeUnit units = tracker.getTickUnits();
        final ListenableFuture<ListenableFuture<T>> source = submit(callable);
        Futures.addCallback(source, new FutureCallback<ListenableFuture<T>>() {
                    @Override
                    public void onSuccess(@Nullable ListenableFuture<T> next) {
                        try {
                            long remaining = tracker.verify();
                            final ScheduledFuture<?> remainingFuture = timers.schedule(new TimeoutTask<>(next, result, remaining, units), remaining, units);
                            Futures.addCallback(next, new FutureCallback<T>() {
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
                            }, ScopedTracingExecutor.this);
                        } catch (TimeoutException e) {
                            next.cancel(true);
                            result.setException(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        result.setException(t);
                    }
                }
        );
        return new WrappedListenableFuture<>(result);
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
            });
            return new WrappedListenableFuture<>(result);
        } else {
            return new WrappedListenableFuture<>(source);
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

    private class WrappedListenableFuture<T> extends ForwardingListenableFuture<T> {
        private final ListenableFuture<T> result;

        public WrappedListenableFuture(ListenableFuture<T> result) {
            this.result = result;
        }

        @Override
        protected ListenableFuture<T> delegate() {
            return result;
        }

        @Override
        public void addListener(Runnable listener, Executor exec) {
            // what is the best way to handle this? for now, force usage of our scoped executor
            super.addListener(listener, ScopedTracingExecutor.this);
        }
    }
}
