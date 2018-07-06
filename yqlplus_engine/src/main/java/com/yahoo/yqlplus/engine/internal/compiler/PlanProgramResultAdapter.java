/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;

import java.util.List;
import java.util.Map;

public class PlanProgramResultAdapter implements ProgramResult, InvocationResultHandler {
    private final Tracer tracer;
    private final List<String> names;
    private final Map<String, SettableFuture<YQLResultSet>> resultSets;
    private final SettableFuture<Tracer> end;

    public PlanProgramResultAdapter(Tracer tracer, List<CompiledProgram.ResultSetInfo> resultSetInfos) {
        this.tracer = tracer;
        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableMap.Builder<String, SettableFuture<YQLResultSet>> resultSets = ImmutableMap.builder();
        for (CompiledProgram.ResultSetInfo info : resultSetInfos) {
            names.add(info.getName());
            resultSets.put(info.getName(), SettableFuture.create());
        }
        this.end = SettableFuture.create();
        this.names = names.build();
        this.resultSets = resultSets.build();
    }

    @Override
    public void fail(Throwable t) {
        end.setException(t);
        for (Map.Entry<String, SettableFuture<YQLResultSet>> e : resultSets.entrySet()) {
            e.getValue().setException(t);
        }
    }

    @Override
    public void succeed(String name, Object value) {
        resultSets.get(name).set(new PlanResultSet(value));
    }

    @Override
    public void fail(String name, Throwable t) {
        resultSets.get(name).setException(t);
    }

    @Override
    public void end() {
        tracer.end();
        end.set(tracer);
    }

    @Override
    public Iterable<String> getResultNames() {
        return names;
    }

    @Override
    public ListenableFuture<YQLResultSet> getResult(String name) {
        return resultSets.get(name);
    }

    @Override
    public ListenableFuture<Tracer> getEnd() {
        return end;
    }

}
