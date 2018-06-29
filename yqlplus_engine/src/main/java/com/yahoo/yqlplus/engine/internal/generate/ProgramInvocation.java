/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.compiler.generate.GambitRuntime;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.YQLRuntimeException;
import com.yahoo.yqlplus.compiler.runtime.TimeoutHandler;
import com.yahoo.yqlplus.engine.internal.scope.ScopedTracingExecutor;

import javax.inject.Named;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

public abstract class ProgramInvocation {
    private volatile InvocationResultHandler resultHandler;

    @Inject
    public Injector injector;

    @Inject
    @Named("programExecutor")
    public ListeningExecutorService tasks;

    @Inject
    @Named("timeout")
    public ScheduledExecutorService timers;

    @Inject
    @Named("rootContext")
    public TaskContext rootContext;

    @Inject
    public TimeoutHandler timeouts;

    public void execute(Runnable runnable) {
        tasks.submit(wrap(runnable));
    }

    private Runnable wrap(final Runnable run) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    run.run();
                } catch (Exception e) {
                    fail(e);
                } catch (Error e) {
                    fail(e);
                    throw e;
                }
            }
        };
    }

    public void executeAll(Runnable one) {
        wrap(one).run();
    }

    public void executeAll(Runnable... runnables) {
        for (int i = 0; i < runnables.length - 1; ++i) {
            execute(runnables[i]);
        }
        wrap(runnables[runnables.length - 1]).run();
    }

    protected void missingArgument(String name, YQLType expectedType) {
        throw new IllegalArgumentException("Missing required program argument '" + name + "' (type '" + expectedType + "')");
    }

    public <ROW> void addAll(List<ROW> rows, List<ROW> inputs) {
        if (inputs == null) {
            return;
        }
        for (ROW row : inputs) {
            if (row != null) {
                rows.add(row);
            }
        }
    }

    public <ROW> void addAll(List<ROW> rows, Iterable<ROW> inputs) {
        if (inputs == null) {
            return;
        }
        for (ROW row : inputs) {
            if (row != null) {
                rows.add(row);
            }
        }
    }

    public <ROW> void addAll(List<ROW> rows, Collection<ROW> inputs) {
        if (inputs == null) {
            return;
        }
        for (ROW row : inputs) {
            if (row != null) {
                rows.add(row);
            }
        }
    }

    public List<Object> singleton(Object input) {
        if (input != null) {
            return ImmutableList.of(input);
        } else {
            return ImmutableList.of();
        }
    }

    public <ROW> List<ROW> sort(List<ROW> rows, Comparator<ROW> comparator) {
        Collections.sort(rows, comparator);
        return rows;
    }

    public void invoke(InvocationResultHandler resultHandler, Map<String, Object> arguments) {
        this.resultHandler = resultHandler;
        try {
            readArguments(arguments);
            run();
        } catch (Exception e) {
            fail(e);
        } catch (Error e) {
            fail(e);
            throw e;
        }
    }

    protected abstract void readArguments(Map<String, Object> arguments);

    public final void succeed(String name, Object out) {
        resultHandler.succeed(name, out);
    }

    public final void fail(String name, Throwable failure) {
        resultHandler.fail(name, extractCause(failure));
    }

    private Throwable extractCause(Throwable failure) {
        while ((failure instanceof YQLRuntimeException || failure instanceof ExecutionException) && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    public final void end() {
        resultHandler.end();
    }

    public final void fail(Throwable t) {
        resultHandler.fail(extractCause(t));
    }

    public final void notComparable(Object target, int line, int offset) {
        throw new YQLTypeException("L" + line + ":" + offset + ": unable to compare " + target);
    }

    protected abstract void run() throws Exception;

    public final Injector getInjector() {
        return injector;
    }

    public final GambitRuntime getRuntime(TaskContext context) {
        final ListeningExecutorService tasks = ((ScopedTracingExecutor) this.tasks).createSubExecutor(context);
        return new GambitRuntime() {
            @Override
            public ListenableFuture<List<Object>> scatter(List<Callable<Object>> targets) {
                List<ListenableFuture<Object>> resultList = Lists.newArrayListWithExpectedSize(targets.size());
                for (Callable<Object> out : targets) {
                    resultList.add(fork(out));
                }
                return Futures.allAsList(resultList);
            }

            @Override
            public ListenableFuture<List<Object>> scatterAsync(List<Callable<ListenableFuture<Object>>> targets) {
                List<ListenableFuture<Object>> resultList = Lists.newArrayListWithExpectedSize(targets.size());
                for (Callable<ListenableFuture<Object>> out : targets) {
                    resultList.add(forkAsync(out));
                }
                return Futures.allAsList(resultList);
            }

            @Override
            public ListenableFuture<Object> fork(Callable<Object> target) {
                return tasks.submit(target);
            }

            @Override
            public ListenableFuture<Object> forkAsync(Callable<ListenableFuture<Object>> target) {
                SettableFuture<Object> result = SettableFuture.create();
                result.setFuture(tasks.submit(target));
                return result;
            }
        };
    }
}
