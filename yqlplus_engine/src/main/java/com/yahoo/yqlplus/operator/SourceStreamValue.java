/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.Collection;

public class SourceStreamValue extends StreamValue {
    private OperatorNode<PhysicalExprOperator> source;

    public SourceStreamValue(ContextPlanner context, OperatorNode<PhysicalExprOperator> source) {
        super(context);
        this.source = source;
    }

    @Override
    public OperatorValue materialize() {
        return OperatorStep.create(context.getValueTypeAdapter(), source.getLocation(), PhysicalOperator.EVALUATE_GUARD, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, source, stream));
    }

    @Override
    public OperatorValue materializeIf(OperatorNode<PhysicalExprOperator> expression) {
        return OperatorStep.create(context.getValueTypeAdapter(), source.getLocation(), PhysicalOperator.EVALUATE_GUARD, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.IF, expression, OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, source, stream), context.constant(ImmutableList.of())));
    }

    @Override
    public Collection<OperatorValue> feedIf(OperatorNode<PhysicalExprOperator> condition, OperatorValue stream) {
        OperatorNode<SinkOperator> sink = OperatorNode.create(SinkOperator.STREAM, OperatorNode.create(PhysicalExprOperator.VALUE, stream));
        return ImmutableList.of(OperatorStep.create(context.getValueTypeAdapter(), source.getLocation(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.IF, condition, OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, source, setSink(this.stream, sink)), context.constant(ImmutableList.of()))));
    }

    @Override
    public Collection<OperatorValue> feed(OperatorValue stream) {
        OperatorNode<SinkOperator> sink = OperatorNode.create(SinkOperator.STREAM, OperatorNode.create(PhysicalExprOperator.VALUE, stream));
        return ImmutableList.of(OperatorStep.create(context.getValueTypeAdapter(), source.getLocation(), PhysicalOperator.EXECUTE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, source, setSink(this.stream, sink))));
    }
}
