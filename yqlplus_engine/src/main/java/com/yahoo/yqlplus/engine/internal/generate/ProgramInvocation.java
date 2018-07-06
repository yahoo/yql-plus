/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.engine.compiler.runtime.YQLRuntimeException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public abstract class ProgramInvocation {
    private volatile InvocationResultHandler resultHandler;


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

    protected abstract void run() throws Exception;
}
