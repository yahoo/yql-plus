/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.tasks;

import com.google.common.collect.*;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan the execution of steps as a graph of Tasks.
 * <p/>
 * Our goal is to split execution into as many parallel tracks as possible while coalescing linear series of steps into
 * linear execution in a single thread.
 * <p/>
 * We also want to identify steps which are async and assemble callbacks for them to run.
 * <p/>
 * A typical CQL program will likely have several largely or entirely disjoint graphs (imagine a yahoo page assembling data from several backends).
 * <p/>
 * Two kinds of nodes:
 * start(steps) - start these steps (no waiting)
 * join(steps, steps) - wait for these steps to finish, then run these steps
 */
public final class GraphPlanner {
    static class Node {
        Set<Value> available = Sets.newIdentityHashSet();
        Set<Step> inputs = Sets.newIdentityHashSet();
        List<Step> todo = Lists.newArrayList();
        Set<Step> deps = Sets.newIdentityHashSet();

        RunTask run;
        Task next;

        Node(Step step) {
            todo.add(step);
        }
    }

    private void discover(Step step, Map<Step, Node> nodes) {
        if (nodes.containsKey(step)) {
            return;
        }
        Node node = new Node(step);
        nodes.put(step, node);
        for (Value val : step.getInputs()) {
            Step source = val.getSource();
            node.inputs.add(source);
            discover(source, nodes);
        }
    }

    private void populateAvailable(Node node, Set<Value> available, Map<Step, Node> nodes) {
        for (Step src : node.todo) {
            available.add(src.getOutput());
        }
        node.available.addAll(available);
        for (Step dst : node.deps) {
            Set<Value> copy = Sets.newIdentityHashSet();
            copy.addAll(available);
            populateAvailable(nodes.get(dst), copy, nodes);
        }
    }

    /**
     * Plan a graph of tasks using the terminal step as a starting point. Discover all of the used steps from those roots, and then return the starting task.
     */
    public ForkTask plan(Step root) {
        Map<Step, Node> nodes = Maps.newIdentityHashMap();

        discover(root, nodes);

        for (Map.Entry<Step, Node> e : nodes.entrySet()) {
            for (Step dep : e.getValue().inputs) {
                nodes.get(dep).deps.add(e.getKey());
            }
        }

        for (Map.Entry<Step, Node> e : nodes.entrySet()) {
            if (e.getValue().inputs.isEmpty()) {
                populateAvailable(e.getValue(), Sets.<Value>newIdentityHashSet(), nodes);
            }
        }

        boolean modified = true;
        List<Step> keys = Lists.newArrayList();
        while (modified) {
            modified = false;
            // we plan to modify the map, so copy the set of keys before iteration
            keys.clear();
            keys.addAll(nodes.keySet());
            for (Step key : keys) {
                Node node = nodes.get(key);
                if (node == null) {
                    continue;
                }
                if (node.deps.size() == 1) {
                    // if we are the only input to that dep...
                    Step dep = Iterables.get(node.deps, 0);
                    if (dep.getInputs().size() == 1) {
                        // then merge into that
                        Node target = nodes.get(dep);
                        node.todo.addAll(target.todo);
                        target.todo = node.todo;
                        target.inputs.remove(key);
                        for (Step p : node.inputs) {
                            nodes.get(p).deps.remove(key);
                            nodes.get(p).deps.add(dep);
                        }
                        target.inputs.addAll(node.inputs);
                        target.available.addAll(node.available);
                        nodes.remove(key);

                        //check if all non-END nodes only have one todo
                        boolean found = false;
                        for (Map.Entry<Step, Node> entry : nodes.entrySet()) {
                            if (entry.getKey() instanceof OperatorStep) {
                                if (!((OperatorStep) entry.getKey()).getCompute().getOperator().equals(PhysicalOperator.END)) {
                                    if (!(entry.getValue().todo.size() == 1)) {
                                        found = true;
                                        break;
                                    }
                                }
                            }
                        }
                        modified = found;
                    }
                }
            }
        }

        // so now all remaining nodes have 0 or > 1 deps
        // the 0 deps nodes are "ending" nodes, and the > deps nodes are fork nodes

        // set up the run & end nodes
        ForkTask start = new ForkTask();


        Map<Set<Step>, JoinTask> joinTasks = Maps.newHashMap();

        for (Node n : nodes.values()) {
            n.run = new RunTask(n.todo);
            n.run.setAvailable(n.available);
            if (n.inputs.size() > 1) {
                Set<Step> key = ImmutableSet.copyOf(n.inputs);
                JoinTask join = joinTasks.get(key);
                if (join == null) {
                    join = new JoinTask();
                    joinTasks.put(key, join);
                    join.setAvailable(Sets.<Value>newIdentityHashSet());
                }
                join.addNext(n.run);
                join.getAvailable().addAll(n.available);
                n.next = join;
            } else {
                n.next = n.run;
            }
        }

        for (Node n : nodes.values()) {
            if (n.inputs.isEmpty()) {
                start.addNext(n.next);
            }
            for (Step dep : n.deps) {
                Node node = nodes.get(dep);
                if (node.next instanceof JoinTask) {
                    ((JoinTask) node.next).getPriors().add(n.run);
                }
                n.run.addNext(node.next);
            }
        }

        return start;
    }
}
