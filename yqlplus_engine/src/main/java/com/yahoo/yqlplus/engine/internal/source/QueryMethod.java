/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.IndexedSourceType;
import com.yahoo.yqlplus.engine.internal.plan.PlanChain;
import com.yahoo.yqlplus.engine.internal.plan.ast.ExprScope;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.StreamOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.StreamValue;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.List;

class QueryMethod extends IndexedMethod {
    public QueryMethod(TypeWidget rowType, TypeWidget adapterType, GambitCreator.Invocable invoker, boolean singleton, boolean async, long minimumBudget, long maximumBudget) {
        this(null, QueryType.SCAN, rowType, adapterType, invoker, singleton, async, minimumBudget, maximumBudget);
    }

    public QueryMethod(IndexDescriptor descriptor, QueryType indexType, TypeWidget rowType, TypeWidget adapterType, GambitCreator.Invocable invoker, boolean singleton, boolean async, long minimumBudget, long maximumBudget) {
        super(minimumBudget, rowType, maximumBudget, invoker, indexType, singleton, async, descriptor);
    }

    public void index(List<StreamValue> out, Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, List<IndexedSourceType.IndexQuery> todo) {
        StreamValue cursor = createKeyCursor(planner, location, todo);
        switch (type) {
            case BATCH: {
                // we're a batch API, so we need to get ALL of the queries (and we're not going to handle any followup filters)
                // we only support a single @Key argument and we need a list of keys
                StreamValue result = executeCall(location, source, planner, cursor.materializeValue());
                if (todo.size() == 1) {
                    IndexedSourceType.IndexQuery q = todo.get(0);
                    if (!q.handledFilter) {
                        result.add(q.filterPredicate.getLocation(), StreamOperator.FILTER, q.filterPredicate);
                        q.handledFilter = true;
                    }
                }
                out.add(result);
                break;
            }
            case SINGLE: {
                // we're NOT a batch API -- each invocation only looks up one keypair
                // so there's no need to batch up all keys
                // except we need to be sure we only emit a given key tuple once... and intersect any filters
                ExprScope functionScope = new ExprScope();
                functionScope.addArgument("$key");
                OperatorNode<FunctionOperator> function = functionScope.createFunction(createInvocation(location, source, planner, OperatorNode.create(PhysicalExprOperator.LOCAL, "$key")));
                cursor.add(source.getLocation(), StreamOperator.SCATTER, function);
                if (!singleton) {
                    cursor.add(source.getLocation(), StreamOperator.FLATTEN);
                }
                OperatorNode<FunctionOperator> isNotNull = OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$"),
                        OperatorNode.create(PhysicalExprOperator.NOT, OperatorNode.create(PhysicalExprOperator.IS_NULL, OperatorNode.create(PhysicalExprOperator.LOCAL, "$$"))));
                cursor.add(source.getLocation(), StreamOperator.FILTER, isNotNull);
                if (todo.size() == 1) {
                    IndexedSourceType.IndexQuery q = todo.get(0);
                    if (!q.handledFilter) {
                        cursor.add(q.filterPredicate.getLocation(), StreamOperator.FILTER, q.filterPredicate);
                        q.handledFilter = true;
                    }
                }
                out.add(cursor);
                break;
            }
            default:
                throw new ProgramCompileException("Invalid type for QueryMethod.index invocation: %s", type);
        }
    }

    private OperatorNode<PhysicalExprOperator> createInvocation(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, OperatorNode<PhysicalExprOperator> key) {
        return super.createInvocation(location, source, planner, key, ImmutableList.of());
    }

    public StreamValue scan(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, PlanChain.LocalChainState state) {
        Preconditions.checkArgument(type == QueryType.SCAN);
        return executeCall(location, source, planner, null);
    }

    private StreamValue executeCall(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, OperatorNode<PhysicalExprOperator> cursor) {
        OperatorNode<PhysicalExprOperator> result = createInvocation(location, source, planner, cursor);
        if (singleton) {
            return StreamValue.singleton(planner, result);
        }
        return StreamValue.iterate(planner, result);
    }

}
