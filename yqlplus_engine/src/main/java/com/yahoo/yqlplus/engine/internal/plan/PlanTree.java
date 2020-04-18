/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.engine.internal.compiler.streams.PlanProgramCompileOptions;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.tasks.*;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanTree {
    static class Next {
        Set<OperatorValue> used = Sets.newIdentityHashSet();
        String name;
        OperatorNode<TaskOperator> operator;

        Next(String name) {
            this.name = name;
        }

        List<OperatorValue> toUsed() {
            return Lists.newArrayList(used);
        }

        public OperatorNode<TaskOperator> toCall() {
            return operator;
        }
    }

    static class PlanState {
        List<OperatorNode<TaskOperator>> tasks = Lists.newArrayList();
        Map<OperatorValue, OperatorNode<TaskOperator>> outputMap = Maps.newIdentityHashMap();
        Map<Task, Next> names = Maps.newIdentityHashMap();
        Map<JoinTask, Next> joins = Maps.newIdentityHashMap();
        int sym = 0;

        void assignName(OperatorValue value) {
            if (value.getName() == null) {
                value.setName(gensym("val"));
            }
        }

        String gensym(String prefix) {
            return prefix + (sym++);
        }

        Next dispatch(Task parent, Task task) {
            if (task instanceof RunTask) {
                RunTask run = (RunTask) task;
                return run(run);
            } else if (task instanceof JoinTask) {
                JoinTask join = (JoinTask) task;
                return join(parent, join);
            } else {
                throw new UnsupportedOperationException("Unknown task type: " + task);
            }
        }

        private void add(Set<OperatorValue> out, Set<? extends Value> available, Set<? extends Value> in) {
            for (Value v : in) {
                if (v instanceof OperatorValue && available.contains(v)) {
                    if (((OperatorValue) v).hasDatum()) {
                        assignName((OperatorValue) v);
                        out.add((OperatorValue) v);
                    }
                }
            }
        }


        Next run(RunTask task) {
            if (names.containsKey(task)) {
                return names.get(task);
            }
            Next next = new Next(gensym("run"));
            add(next.used, task.getAvailable(), task.getInputs());
            List<Next> nexts = createNexts(task, next);
            List<OperatorStep> steps = Lists.newArrayList();
            Set<OperatorValue> outputs = Sets.newIdentityHashSet();
            for (Step step : task.getSteps()) {
                if (step instanceof OperatorStep) {
                    steps.add((OperatorStep) step);
                    OperatorValue output = (OperatorValue) step.getOutput();
                    if (output.hasDatum()) {
                        outputs.add(output);
                    }
                    next.used.remove(step.getOutput());
                } else {
                    System.err.println("Not an OperatorStep?: " + step);
                }
            }
            OperatorNode<TaskOperator> node = OperatorNode.create(TaskOperator.RUN, next.name, next.toUsed(), steps, createNext(nexts));
            for (OperatorValue output : outputs) {
                outputMap.put(output, node);
            }
            tasks.add(node);
            names.put(task, next);
            next.operator = OperatorNode.create(TaskOperator.CALL, next.name, next.toUsed());
            return next;
        }

        Next join(Task parent, JoinTask task) {
            Next join;
            if (joins.containsKey(task)) {
                join = joins.get(task);
            } else {
                join = new Next(gensym("join"));
                add(join.used, task.getAvailable(), task.getInputs());
                List<Next> nextNames = createNexts(task, join);
                OperatorNode<TaskOperator> node = OperatorNode.create(TaskOperator.JOIN, join.name, join.toUsed(), createNext(nextNames), task.getPriors().size());
                join.operator = node;
                tasks.add(node);
                joins.put(task, join);
            }
            Next ready = new Next(gensym("ready"));
            add(ready.used, parent.getAvailable(), join.used);
            OperatorNode<TaskOperator> node = OperatorNode.create(TaskOperator.READY, join.name, ready.toUsed());
            ready.operator = node;
            return ready;

        }

        private List<Next> createNexts(Task task, Next current) {
            List<Next> nextNames = Lists.newArrayList();
            for (Task next : task.getNext()) {
                Next v = dispatch(task, next);
                nextNames.add(v);
                add(current.used, task.getAvailable(), v.used);
            }
            return nextNames;
        }

        private OperatorNode<TaskOperator> createNext(List<Next> nextNames) {
            if (nextNames.isEmpty()) {
                return OperatorNode.create(TaskOperator.END);
            } else if (nextNames.size() == 1) {
                return nextNames.get(0).toCall();
            } else {
                List<OperatorNode<TaskOperator>> names = Lists.newArrayList();
                for (Next n : nextNames) {
                    names.add(n.toCall());
                }
                return OperatorNode.create(TaskOperator.PARALLEL, names);
            }
        }

        OperatorNode<TaskOperator> fork(ForkTask task) {
            Next next = new Next("fork");
            List<Next> nexts = createNexts(task, next);
            return createNext(nexts);
        }
    }

    public OperatorNode<TaskOperator> planTask(List<OperatorNode<TaskOperator>> arguments, Step root, PlanProgramCompileOptions planProgramCompileOptions) {
        GraphPlanner graphPlanner = new GraphPlanner();
        ForkTask fork = graphPlanner.plan(root, planProgramCompileOptions);
        PlanState state = new PlanState();
        OperatorNode<TaskOperator> start = state.fork(fork);
        return OperatorNode.create(TaskOperator.PLAN, start, arguments, new Sorter(state.outputMap, state.tasks).sort(start));
    }

    /**
     * Topo-sort all of the tasks so that all values will be defined before they are referenced.
     */
    static class Sorter {
        Map<OperatorValue, OperatorNode<TaskOperator>> outputMap;
        Map<String, OperatorNode<TaskOperator>> taskMap;
        Set<String> seen;
        List<OperatorNode<TaskOperator>> sortedTasks;

        public Sorter(Map<OperatorValue, OperatorNode<TaskOperator>> outputMap, List<OperatorNode<TaskOperator>> tasks) {
            this.taskMap = Maps.newLinkedHashMap();
            for (OperatorNode<TaskOperator> task : tasks) {
                String name = task.getArgument(0);
                taskMap.put(name, task);
            }
            this.sortedTasks = Lists.newArrayListWithCapacity(tasks.size());
            this.outputMap = outputMap;
            this.seen = Sets.newHashSet();
        }

        public List<OperatorNode<TaskOperator>> sort(OperatorNode<TaskOperator> start) {
            visitCall(start);
            return this.sortedTasks;
        }

        private void visitCall(OperatorNode<TaskOperator> call) {
            switch (call.getOperator()) {
                case PARALLEL: {
                    List<OperatorNode<TaskOperator>> nexts = call.getArgument(0);
                    for (OperatorNode<TaskOperator> next : nexts) {
                        visitCall(next);
                    }
                    break;
                }
                case END:
                    break;
                case READY:
                case CALL: {
                    String name = call.getArgument(0);
                    OperatorNode<TaskOperator> task = taskMap.get(name);
                    visitTask(task);
                    if (task != null) {
                        switch (task.getOperator()) {
                            case END:
                                break;
                            case RUN: {
                                OperatorNode<TaskOperator> next = task.getArgument(3);
                                visitCall(next);
                                break;
                            }
                            case JOIN: {
                                OperatorNode<TaskOperator> next = task.getArgument(2);
                                visitCall(next);
                                break;
                            }
                            default:
                                throw new UnsupportedOperationException("unexpected task type: " + task);
                        }
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException("unexpected call: " + call);
            }
        }

        private void visitTask(OperatorNode<TaskOperator> task) {
            List<OperatorValue> inputs = task.getArgument(1);
            for (OperatorValue input : inputs) {
                OperatorNode<TaskOperator> outputTask = outputMap.get(input);
                visitTask(outputTask);
                addTask(outputTask);
            }
            addTask(task);

        }

        private void addTask(OperatorNode<TaskOperator> outputTask) {
            String name = outputTask.getArgument(0);
            if (!seen.contains(name)) {
                seen.add(name);
                sortedTasks.add(outputTask);
            }
        }
    }


}
