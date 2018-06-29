/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.base.Charsets;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.engine.internal.code.CodeOutput;
import com.yahoo.yqlplus.engine.internal.generate.ProgramInvocation;
import com.yahoo.yqlplus.compiler.runtime.ProgramTracer;
import com.yahoo.yqlplus.compiler.runtime.RelativeTicker;
import com.yahoo.yqlplus.compiler.runtime.TimeoutTracker;
import com.yahoo.yqlplus.engine.internal.plan.PlanPrinter;
import com.yahoo.yqlplus.engine.internal.plan.TaskOperator;
import com.yahoo.yqlplus.engine.internal.scope.ExecutionScoper;
import com.yahoo.yqlplus.engine.internal.scope.ScopedObjects;
import com.yahoo.yqlplus.engine.internal.scope.ScopedTracingExecutor;
import com.yahoo.yqlplus.engine.scope.EmptyExecutionScope;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;
import com.yahoo.yqlplus.engine.scope.WrapScope;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class PlanCompiledProgram implements CompiledProgram {
    private final String name;
    private final List<ArgumentInfo> argumentInfos;
    private final List<ResultSetInfo> resultSetInfos;
    private final Map<String, OperatorNode<SequenceOperator>> views;
    private final byte[] dump;
    private final OperatorNode<TaskOperator> plan;
    private final Class<? extends ProgramInvocation> compiledProgram;


    PlanCompiledProgram(String name, List<ArgumentInfo> argumentInfos, List<ResultSetInfo> resultSetInfos, Map<String, OperatorNode<SequenceOperator>> views, OperatorNode<TaskOperator> plan, byte[] dump, Class<? extends ProgramInvocation> compiledProgram) {
        this.name = name;
        this.argumentInfos = argumentInfos;
        this.resultSetInfos = resultSetInfos;
        this.views = views;
        this.plan = plan;
        this.dump = dump;
        this.compiledProgram = compiledProgram;
    }

    @Inject
    protected ExecutionScoper scoper;

    @Inject
    @Named("timeout")
    protected ScheduledExecutorService timerExecutor;

    @Inject
    @Named("work")
    protected ExecutorService workExecutor;

    @Inject
    protected Injector injector;

    public String getName() {
        return name;
    }


    public OperatorNode<TaskOperator> getPlan() {
        return plan;
    }

    @Override
    public List<ArgumentInfo> getArguments() {
        return argumentInfos;
    }

    @Override
    public List<ResultSetInfo> getResultSets() {
        return resultSetInfos;
    }

    @Override
    public List<String> getViewNames() {
        return ImmutableList.copyOf(views.keySet());
    }

    public Class<? extends ProgramInvocation> getCompiledProgram() {
        return compiledProgram;
    }

    @Override
    public OperatorNode<SequenceOperator> getView(String name) {
        return views.get(name);
    }

    @Override
    public ProgramResult run(final Map<String, Object> arguments, final boolean debug) throws Exception {
        return run(arguments, debug, new EmptyExecutionScope());
    }
    
    @Override
    public ProgramResult run(final Map<String, Object> arguments, final boolean debug, ExecutionScope inputScope) throws Exception {
        return run(arguments, debug, inputScope, 30L, TimeUnit.SECONDS);
    }
    
    @Override
    public ProgramResult run(Map<String, Object> arguments, boolean debug, long timeout, TimeUnit timeoutUnit) throws Exception {
        return run(arguments, debug, new EmptyExecutionScope(), timeout, timeoutUnit);
    }

    @Override
    public ProgramResult run(Map<String, Object> arguments, boolean debug, ExecutionScope inputScope, long timeout, TimeUnit timeoutUnit) {
        TimeoutTracker tracker = new TimeoutTracker(timeout, timeoutUnit, new RelativeTicker(Ticker.systemTicker()));
        ProgramTracer tracer = new ProgramTracer(Ticker.systemTicker(), debug, "program", name);
        scoper.enter(new ScopedObjects(inputScope));
        TaskMetricEmitter requestEmitter = injector.getInstance(TaskMetricEmitter.class);
        TaskContext context = new TaskContext(requestEmitter, tracer, tracker);
        PlanProgramResultAdapter adapter = new PlanProgramResultAdapter(tracer, resultSetInfos, scoper);
        invoke(adapter, arguments, inputScope, context);
        requestEmitter.end();
        scoper.exit();
        return adapter;
    }

    @Override
    public void invoke(final InvocationResultHandler resultHandler, final Map<String, Object> arguments, ExecutionScope inputScope, final TaskContext context) {
        ExecutionScope scope = new WrapScope(inputScope)
                .bind(Boolean.class, "debug", true)
                .bind(String.class, "programName", name);
       // final TaskMetricEmitter programTasks = context.metricEmitter; //.start("program.tmp", name); 
        ScopedTracingExecutor tracingExecutor = new ScopedTracingExecutor(timerExecutor, workExecutor, scoper, context.metricEmitter, context.tracer, context.timeout, scope);
        final Injector injector = this.injector;
        tracingExecutor.runNow(new Runnable() {
            @Override
            public void run() {
                ProgramInvocation program = injector.getInstance(getCompiledProgram());
                program.invoke(resultHandler, arguments);
            }
        });
    }

    @Override
    public void dump(OutputStream out) throws IOException {
        Writer output = new OutputStreamWriter(out, Charsets.UTF_8);
        CodeOutput dumper = new CodeOutput();
        PlanPrinter printer = new PlanPrinter();
        printer.dump(dumper, plan);
        output.write(dumper.toDumpString());
        output.write("\n\n");
        output.flush();
        out.write(dump);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsStatement(ProgramStatement programStatement) {
        Object statements = plan.getAnnotation("yql.writeStatements");
        if (statements != null) {
            return ((Set<ProgramStatement>) statements).contains(programStatement);
        }
        return false;
    }
}
