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
import com.google.inject.Key;
import com.yahoo.yqlplus.api.trace.TraceRequest;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.compiler.runtime.ProgramTracer;
import com.yahoo.yqlplus.engine.internal.scope.ExecutionScoper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PlanProgramResultAdapter implements ProgramResult, InvocationResultHandler {
    private final ProgramTracer tracer;
    private final List<String> names;
    private final Map<String, SettableFuture<YQLResultSet>> resultSets;
    private final SettableFuture<TraceRequest> end;
    private final ExecutionScoper scoper;
    private Map<Key<?>, Object> scopedObjects;

    public PlanProgramResultAdapter(ProgramTracer tracer, List<CompiledProgram.ResultSetInfo> resultSetInfos, ExecutionScoper scoper) {
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
        this.scoper = scoper;
    }

    @Override
    public void fail(Throwable t) {
        scopedObjects = scoper.getScope().getScopedObjects();
        end.setException(t);
        for (Map.Entry<String, SettableFuture<YQLResultSet>> e : resultSets.entrySet()) {
            e.getValue().setException(t);
        }
    }

    @Override
    public void succeed(String name, Object value) {
        scopedObjects = scoper.getScope().getScopedObjects();
        resultSets.get(name).set(new PlanResultSet(value));
    }

    @Override
    public void fail(String name, Throwable t) {
        scopedObjects = scoper.getScope().getScopedObjects();
        resultSets.get(name).setException(t);
    }

    @Override
    public void end() {
        end.set(tracer.createTrace());
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
    public ListenableFuture<TraceRequest> getEnd() {
        return end;
    }

    @Override
    public Collection<Object> getExecuteScopedObjects() {
        return Collections.unmodifiableCollection(null == scopedObjects ? Collections.emptyMap().values() : scopedObjects.values());
    }
}
