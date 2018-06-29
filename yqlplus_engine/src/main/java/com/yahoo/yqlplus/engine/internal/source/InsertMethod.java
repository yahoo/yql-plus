/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.compiler.generate.GambitCreator;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.PlanChain;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;
import java.util.concurrent.TimeUnit;

class InsertMethod {
    private final String methodName;
    private final TypeWidget rowType;
    private final YQLStructType insertRecord;
    private final TypeWidget adapterType;
    private final boolean async;
    private final boolean singleton;
    private final GambitCreator.Invocable invoker;
    private final long minimumBudget;
    private final long maximumBudget;
    private final boolean batch;

    public InsertMethod(String methodName, TypeWidget rowType, YQLStructType recordType, TypeWidget adapterType, GambitCreator.Invocable invoker, boolean batch, boolean singleton, boolean async, long minimumBudget, long maximumBudget) {
        this.methodName = methodName;
        this.rowType = rowType;
        this.insertRecord = recordType;
        this.adapterType = adapterType;
        this.invoker = invoker;
        this.singleton = singleton;
        this.async = async;
        this.minimumBudget = minimumBudget;
        this.maximumBudget = maximumBudget;
        this.batch = batch;
    }

    public StreamValue insert(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, PlanChain.LocalChainState state, StreamValue records) {
        // TODO: should we validate that the inserted records don't contain any unknown fields?
        if(batch) {
            OperatorNode<PhysicalExprOperator> result = createInvocation(location, source, planner, records.materializeValue());
            if (singleton) {
                return StreamValue.singleton(planner, result);
            }
            return StreamValue.iterate(planner, result);
        } else {
            StreamValue input = records;
            input.add(Location.NONE, StreamOperator.TRANSFORM,
                    OperatorNode.create(Location.NONE, FunctionOperator.FUNCTION, ImmutableList.of("$row"),
                            createInvocation(location, source, planner, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"))));
            if(!singleton) {
                input.add(Location.NONE, StreamOperator.FLATTEN);
            }
            return input;
        }
    }

    private OperatorNode<PhysicalExprOperator> createInvocation(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, OperatorNode<PhysicalExprOperator> recordOrRecords) {
        List<OperatorNode<PhysicalExprOperator>> callArgs = Lists.newArrayListWithExpectedSize(2);
        callArgs.add(source);
        callArgs.add(OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT));
        callArgs.add(recordOrRecords);
        OperatorNode<PhysicalExprOperator> invocation = OperatorNode.create(location,
                invoker.getReturnType().isPromise() ? PhysicalExprOperator.ASYNC_INVOKE : PhysicalExprOperator.INVOKE,
                invoker,
                callArgs);
        if (minimumBudget > 0 || maximumBudget > 0) {
            OperatorNode<PhysicalExprOperator> ms = planner.constant(TimeUnit.MILLISECONDS);
            OperatorNode<PhysicalExprOperator> subContext = OperatorNode.create(PhysicalExprOperator.TIMEOUT_GUARD, planner.constant(minimumBudget), ms, planner.constant(maximumBudget), ms);
            return OperatorNode.create(PhysicalExprOperator.WITH_CONTEXT, subContext, OperatorNode.create(PhysicalExprOperator.ENFORCE_TIMEOUT, invocation));
        }
        return invocation;
    }
}
