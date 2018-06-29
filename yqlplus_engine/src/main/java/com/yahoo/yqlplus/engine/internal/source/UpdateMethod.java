/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.api.types.YQLNamePair;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.engine.api.Record;
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
import com.yahoo.yqlplus.compiler.code.MapTypeWidget;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class UpdateMethod extends IndexedMethod {
    private final String methodName;
    private final YQLStructType updateRecord;
    private final TypeWidget updateType;

    public UpdateMethod(String methodName, TypeWidget rowType, YQLStructType updateRecord, TypeWidget updateType, TypeWidget adapterType, GambitCreator.Invocable invoker, boolean singleton, boolean async, long minimumBudget, long maximumBudget) {
        this(methodName, null, QueryType.SCAN, rowType, updateRecord, updateType, adapterType, invoker, singleton, async, minimumBudget, maximumBudget);
    }

    public UpdateMethod(String methodName, IndexDescriptor descriptor, QueryType indexType, TypeWidget rowType, YQLStructType updateRecord, TypeWidget updateType, TypeWidget adapterType, GambitCreator.Invocable invoker, boolean singleton, boolean async, long minimumBudget, long maximumBudget) {
        super(minimumBudget, rowType, maximumBudget, invoker, indexType, singleton, async, descriptor);
        this.methodName = methodName;
        this.updateRecord = updateRecord;
        this.updateType = updateType;
    }

    public void index(List<StreamValue> out, Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, List<IndexedSourceType.IndexQuery> todo, OperatorNode<PhysicalExprOperator> record) {
        StreamValue cursor = createKeyCursor(planner, location, todo);
        switch (type) {
            case BATCH: {
                // we're a batch API, so we need to get ALL of the queries (and we're not going to handle any followup filters)
                // we only support a single @Key argument and we need a list of keys
                StreamValue result = executeCall(location, source, planner, cursor.materializeValue(), record);
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
                // for this case, we start with a stream of keys
                StreamValue result = cursor;
                // and then scatter
                ExprScope functionScope = new ExprScope();
                functionScope.addArgument("$key");
                OperatorNode<FunctionOperator> function = functionScope.createFunction(createInvocation(location, source, planner, OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"), record));
                result.add(source.getLocation(), StreamOperator.SCATTER, function);
                if (!singleton) {
                    result.add(source.getLocation(), StreamOperator.FLATTEN);
                }
                OperatorNode<FunctionOperator> isNotNull = OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$"),
                        OperatorNode.create(PhysicalExprOperator.NOT, OperatorNode.create(PhysicalExprOperator.IS_NULL, OperatorNode.create(PhysicalExprOperator.LOCAL, "$$"))));
                cursor.add(source.getLocation(), StreamOperator.FILTER, isNotNull);
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
            default:
                throw new ProgramCompileException("Invalid type for QueryMethod.index invocation: %s", type);
        }
    }

    private OperatorNode<PhysicalExprOperator> createInvocation(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, OperatorNode<PhysicalExprOperator> key, OperatorNode<PhysicalExprOperator> record) {
        List<String> fieldNames = null;
        List<OperatorNode<PhysicalExprOperator>> fieldValues = null;
        if (PhysicalExprOperator.CONSTANT == record.getOperator()) {
            Preconditions.checkArgument(record.getArgument(0) instanceof MapTypeWidget, "Operator %s is not constant map", record);
            Record constantArg = record.getArgument(1);
            fieldNames = ImmutableList.copyOf(constantArg.getFieldNames());
            fieldValues = new ArrayList<>(fieldNames.size());
            for (String fieldName:fieldNames) {
                fieldValues.add(planner.constant(constantArg.get(fieldName)));
            }
        } else {
            fieldNames = record.getArgument(0);
            fieldValues = record.getArgument(1);
        }
        Set<String> fieldNamesCase = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Set<String> structNamesCase = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        fieldNamesCase.addAll(fieldNames);
        for (YQLNamePair field : updateRecord.getFields()) {
            if (field.isRequired() && !fieldNamesCase.contains(field.getName())) {
                throw new ProgramCompileException(location, "UPDATE required field '%s' missing value for method %s", field.getName(), methodName);
            }
            structNamesCase.add(field.getName());
        }
        if (updateRecord.isClosed()) {
            for (String fieldName : fieldNames) {
                if (!structNamesCase.contains(fieldName)) {
                    throw new ProgramCompileException(location, "UPDATE unknown field '%s' for method %s", fieldName, methodName);
                }
            }
        }
        OperatorNode<PhysicalExprOperator> recordAs = OperatorNode.create(record.getLocation() != null ? record.getLocation() : location, PhysicalExprOperator.RECORD_AS, updateType, fieldNames, fieldValues);
        return super.createInvocation(location, source, planner, key, ImmutableList.of(recordAs));
    }

    public StreamValue all(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, PlanChain.LocalChainState state, OperatorNode<PhysicalExprOperator> record) {
        Preconditions.checkArgument(type == QueryType.SCAN);
        return executeCall(location, source, planner, null, record);
    }

    private StreamValue executeCall(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, OperatorNode<PhysicalExprOperator> cursor, OperatorNode<PhysicalExprOperator> record) {
        OperatorNode<PhysicalExprOperator> result = createInvocation(location, source, planner, cursor, record);
        if (singleton) {
            return StreamValue.singleton(planner, result);
        }
        return StreamValue.iterate(planner, result);
    }
}
