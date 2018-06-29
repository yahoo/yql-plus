/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.yahoo.yqlplus.compiler.code.JoinGenerator;
import com.yahoo.yqlplus.compiler.code.TaskGenerator;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.compiler.generate.ASMClassSource;
import com.yahoo.yqlplus.compiler.generate.GambitScope;
import com.yahoo.yqlplus.compiler.generate.GambitSource;
import com.yahoo.yqlplus.compiler.generate.ObjectBuilder;
import com.yahoo.yqlplus.compiler.generate.ScopedBuilder;
import com.yahoo.yqlplus.engine.internal.generate.ProgramGenerator;
import com.yahoo.yqlplus.engine.internal.generate.ProgramInvocation;
import com.yahoo.yqlplus.engine.internal.plan.TaskOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

public class ProgramEnvironment {
    String name;
    ProgramGenerator program;
    Map<String, TaskGenerator> tasks = Maps.newLinkedHashMap();
    Map<String, JoinGenerator> joins = Maps.newLinkedHashMap();
    Map<String, ObjectBuilder.FieldBuilder> joinFields = Maps.newLinkedHashMap();
    ASMClassSource classSource;
    GambitScope scope;

    private OperatorNode<TaskOperator> plan;

    public ProgramEnvironment(String name, ASMClassSource source) {
        this.classSource = source;
        this.scope = new GambitSource(classSource);
        this.program = new ProgramGenerator(scope);
        this.name = name;
    }


    public JoinGenerator createJoin(String name, int count) {
        JoinGenerator join = new JoinGenerator(program.getProgram().type(), scope, count);
        joinFields.put(name, program.addJoin(name, join.getType()));
        joins.put(name, join);
        return join;
    }

    public TaskGenerator createTask(String name) {
        TaskGenerator generator = new TaskGenerator(program, scope);
        tasks.put(name, generator);
        return generator;
    }

    public CompiledProgram compile(Injector injector) {
        try {
            classSource.build();
            Class<? extends ProgramInvocation> programClazz = (Class<? extends ProgramInvocation>) scope.getObjectClass(program.getProgram());
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            classSource.dump(stream);
            byte[] dump = stream.toByteArray();
            PlanCompiledProgram compiledProgram = new PlanCompiledProgram(name, program.getArgumentInfos(), program.getResultSetInfos(), ImmutableMap.of(), plan, dump, programClazz);
            injector.injectMembers(compiledProgram);
            return compiledProgram;
        } catch (ClassNotFoundException e) {
            classSource.trace(System.err);
            throw new ProgramCompileException(e);
        }
    }

    public void setPlan(OperatorNode<TaskOperator> plan) {
        this.plan = plan;
    }

    public OperatorNode<TaskOperator> getPlan() {
        return plan;
    }


    public BytecodeExpression callRunnable(ScopedBuilder body, OperatorNode<TaskOperator> next) {
        Preconditions.checkArgument(next.getOperator() == TaskOperator.CALL);
        String name = next.getArgument(0);
        List<OperatorValue> args = next.getArgument(1);
        Preconditions.checkArgument(tasks.containsKey(name));
        return tasks.get(name).createRunnable(body, body.local("$program"), args);
    }

    public BytecodeExpression readyRunnable(ScopedBuilder body, OperatorNode<TaskOperator> next) {
        Preconditions.checkArgument(next.getOperator() == TaskOperator.READY);
        String name = next.getArgument(0);
        List<OperatorValue> args = next.getArgument(1);
        Preconditions.checkArgument(joins.containsKey(name));
        return joins.get(name).createRunnable(body, joinFields.get(name).get(body.local("$program")), args);
    }

    public ScopedBuilder getStartBody() {
        return program.getBody();
    }
}
