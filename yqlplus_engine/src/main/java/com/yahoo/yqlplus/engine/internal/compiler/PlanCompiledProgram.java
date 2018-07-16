/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.engine.compiler.runtime.ProgramInvocation;
import com.yahoo.yqlplus.engine.internal.code.CodeOutput;
import com.yahoo.yqlplus.engine.internal.plan.PlanPrinter;
import com.yahoo.yqlplus.engine.internal.plan.TaskOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class PlanCompiledProgram implements CompiledProgram {
    private final String name;
    private final List<ArgumentInfo> argumentInfos;
    private final List<ResultSetInfo> resultSetInfos;
    private final Map<String, OperatorNode<SequenceOperator>> views;
    private final byte[] dump;
    private final OperatorNode<TaskOperator> plan;
    private final ProgramCreator compiledProgram;

    private interface ProgramCreator {
        ProgramInvocation create(TaskContext context);
    }

    PlanCompiledProgram(String name, List<ArgumentInfo> argumentInfos, List<ResultSetInfo> resultSetInfos, Map<String, OperatorNode<SequenceOperator>> views, OperatorNode<TaskOperator> plan, byte[] dump, Class<? extends ProgramInvocation> compiledProgram) {
        this.name = name;
        this.argumentInfos = argumentInfos;
        this.resultSetInfos = resultSetInfos;
        this.views = views;
        this.plan = plan;
        this.dump = dump;
        final Constructor<? extends ProgramInvocation> constructor;
        try {
            constructor = compiledProgram.getConstructor(TaskContext.class);
        } catch (NoSuchMethodException e) {
            throw new ProgramCompileException(e);
        }
        this.compiledProgram = (ctx) -> {
            try {
                return constructor.newInstance(ctx);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new ProgramCompileException(e);
            }
        };
    }

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

    public ProgramInvocation getCompiledProgram(TaskContext rootContext) {
        return compiledProgram.create(rootContext);
    }

    @Override
    public OperatorNode<SequenceOperator> getView(String name) {
        return views.get(name);
    }

    private TaskContext defaultTaskContext() {
        return TaskContext.builder()
                .withTimeout(30L, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public ProgramResult run(final Map<String, Object> arguments) throws Exception {
        return run(arguments, defaultTaskContext());
    }

    @Override
    public ProgramResult run(Map<String, Object> arguments, TaskContext context) throws Exception {
        PlanProgramResultAdapter adapter = new PlanProgramResultAdapter(context.tracer, resultSetInfos);
        invoke(adapter, arguments, context);
        return adapter;
    }

    @Override
    public void invoke(final InvocationResultHandler resultHandler, final Map<String, Object> arguments, final TaskContext context) {
        ProgramInvocation program = getCompiledProgram(context);
        program.invoke(resultHandler, arguments);
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
