/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramCompiler;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitCreator;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitScope;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitSource;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ScopedBuilder;
import com.yahoo.yqlplus.engine.internal.generate.ProgramInvocation;
import com.yahoo.yqlplus.engine.internal.plan.ModuleNamespace;
import com.yahoo.yqlplus.engine.internal.plan.ProgramPlanner;
import com.yahoo.yqlplus.engine.internal.plan.SourceNamespace;
import com.yahoo.yqlplus.engine.internal.plan.TaskOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.rules.LogicalTransforms;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import org.antlr.v4.runtime.RecognitionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class PlanProgramCompiler implements ProgramCompiler {
    private Provider<CompilerInstance> provider;

    static class CompilerInstance {
        private final Injector injector;
        private final ASMClassSource classSource;
        private final GambitScope gambitScope;
        private final ProgramPlanner planner;

        @Inject
        CompilerInstance(ASMClassSource classSource, Injector injector, LogicalTransforms transforms, SourceNamespace sourceNamespace, ModuleNamespace moduleNamespace, ViewRegistry viewNamespace) {
            this.classSource = classSource;
            this.gambitScope = new GambitSource(classSource);
            this.planner = new ProgramPlanner(transforms, sourceNamespace, moduleNamespace, gambitScope, viewNamespace);
            this.injector = injector;
        }

        private CompiledProgram compilePlan(ProgramEnvironment environment, OperatorNode<TaskOperator> plan) {
            environment.setPlan(plan);
//            PlanPrinter printer = new PlanPrinter();
//            CodeOutput out = new CodeOutput();
//            printer.dump(out, plan);
//            System.err.print(out.toDumpString());
            OperatorNode<TaskOperator> start = plan.getArgument(0);
            List<OperatorNode<TaskOperator>> tasks = plan.getArgument(2);

            // program instance
            //   arguments
            //   globals
            //   join instances
            //   task_method(arguments)

            // task instance
            //   all arguments
            //

            Map<String, TaskGenerator> taskGeneratorMap = Maps.newLinkedHashMap();
            Map<String, JoinGenerator> joinGeneratorMap = Maps.newLinkedHashMap();

            // one loop to define all OutputValues; then...
            for (OperatorNode<TaskOperator> task : tasks) {
                switch (task.getOperator()) {
                    case RUN: {
                        String name = task.getArgument(0);
                        List<OperatorValue> used = task.getArgument(1);
                        List<OperatorStep> steps = task.getArgument(2);
                        taskGeneratorMap.put(name, compileRun(environment, name, used, steps));
                        break;
                    }
                    case JOIN:
                        String name = task.getArgument(0);
                        List<OperatorValue> used = task.getArgument(1);
                        int count = task.getArgument(3);
                        joinGeneratorMap.put(name, compileJoin(environment, name, used, count));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unexpected task operator: " + task);
                }
            }

            // now all tasks are defined so we can attach all the "next" steps.
            for (OperatorNode<TaskOperator> task : tasks) {
                switch (task.getOperator()) {
                    case RUN: {
                        String name = task.getArgument(0);
                        OperatorNode<TaskOperator> next = task.getArgument(3);
                        invokeRunnables(taskGeneratorMap.get(name).getBody(), environment, next);
                        break;
                    }
                    case JOIN:
                        String name = task.getArgument(0);
                        OperatorNode<TaskOperator> next = task.getArgument(2);
                        invokeRunnables(joinGeneratorMap.get(name).getBody(), environment, next);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unexpected task operator: " + task);
                }
            }
            invokeRunnables(environment.getStartBody(), environment, start);
            return environment.compile(injector);
        }

        public CompiledProgram compile(String programName, String program) throws IOException, RecognitionException {
            return compilePlan(new ProgramEnvironment(programName, classSource), planner.plan(programName, program));
        }


        public CompiledProgram compile(String programName, InputStream program) throws IOException, RecognitionException {
            return compilePlan(new ProgramEnvironment(programName, classSource), planner.plan(programName, program));
        }

        public CompiledProgram compile(OperatorNode<StatementOperator> program) throws IOException {
            Location loc = program.getLocation();
            String programName = loc != null ? loc.getProgramName() : "<string>";
            return compilePlan(new ProgramEnvironment(programName, classSource), planner.plan(program));
        }

        private JoinGenerator compileJoin(ProgramEnvironment environment, String name, List<OperatorValue> used, int count) {
            JoinGenerator join = environment.createJoin(name, count);
            for (OperatorValue arg : used) {
                join.addValue(arg.getName(), environment.program.getValue(arg));
            }
            return join;
        }

        private TaskGenerator compileRun(ProgramEnvironment environment, String name, List<OperatorValue> used, List<OperatorStep> steps) {
            TaskGenerator generator = environment.createTask(name);
            for (OperatorValue arg : used) {
                generator.addArgument(arg);
            }
            for (OperatorStep step : steps) {
                generator.executeStep(step);
            }
            return generator;
        }

        private void invokeRunnables(ScopedBuilder body, ProgramEnvironment environment, OperatorNode<TaskOperator> next) {
            List<BytecodeExpression> runnables = Lists.newArrayList();
            createRunnable(runnables, body, environment, next);
            if (!runnables.isEmpty()) {
                TypeWidget runnableType = body.adapt(Runnable.class, false);
                BytecodeExpression runnableExpression = runnables.size() == 1 ? runnables.get(0) : body.array(next.getLocation(), runnableType, runnables);
                List<TypeWidget> types = ImmutableList.of(runnables.size() == 1 ? runnableType : runnableExpression.getType());
                GambitCreator.Invocable target = body.findExactInvoker(ProgramInvocation.class, "executeAll", BaseTypeAdapter.VOID,
                        types);
                body.exec(body.invoke(next.getLocation(), target, body.local("$program"), runnableExpression));
            }
        }

        private void createRunnable(List<BytecodeExpression> runnables, ScopedBuilder body, ProgramEnvironment environment, OperatorNode<TaskOperator> next) {
            switch (next.getOperator()) {
                case END:
                    break;
                case READY:
                    runnables.add(environment.readyRunnable(body, next));
                    break;
                case CALL:
                    runnables.add(environment.callRunnable(body, next));
                    break;
                case PARALLEL: {
                    List<OperatorNode<TaskOperator>> nexts = next.getArgument(0);
                    for (OperatorNode<TaskOperator> task : nexts) {
                        createRunnable(runnables, body, environment, task);
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unexpected next invocable: " + next.toString());
            }
        }
    }


    @Inject
    PlanProgramCompiler(Provider<CompilerInstance> provider) {
        this.provider = provider;
    }


    public CompiledProgram compile(String programName, String program) throws IOException, RecognitionException {
        return provider.get().compile(programName, program);
    }


    public CompiledProgram compile(String programName, InputStream program) throws IOException, RecognitionException {
        return provider.get().compile(programName, program);
    }

    @Override
    public CompiledProgram compile(OperatorNode<StatementOperator> program) throws IOException {
        return provider.get().compile(program);
    }


}
