/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.Collection;
import java.util.List;

public class MergeStreamValue extends StreamValue {
    private final List<StreamValue> inputStreams;

    public MergeStreamValue(ContextPlanner context, List<StreamValue> inputStreams) {
        super(context);
        this.inputStreams = inputStreams;
    }


    @Override
    public OperatorValue materialize() {
        OperatorValue stream = OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_CREATE, this.stream));
        List<OperatorValue> steps = Lists.newArrayList();
        for (StreamValue val : inputStreams) {
            steps.addAll(val.feed(stream));
        }
        return OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_COMPLETE, OperatorNode.create(PhysicalExprOperator.VALUE, stream), steps));
    }

    @Override
    public OperatorValue materializeIf(OperatorNode<PhysicalExprOperator> expression) {
        OperatorValue stream = OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_CREATE, this.stream));
        List<OperatorValue> steps = Lists.newArrayList();
        for (StreamValue val : inputStreams) {
            steps.addAll(val.feedIf(expression, stream));
        }
        return OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_COMPLETE, OperatorNode.create(PhysicalExprOperator.VALUE, stream), steps));
    }

    @Override
    public Collection<OperatorValue> feed(OperatorValue stream) {
        if (this.stream.getOperator() == StreamOperator.SINK) {
            // no operations tied to this stream -- feed our children directly into that stream
            List<OperatorValue> out = Lists.newArrayList();
            for (StreamValue val : inputStreams) {
                out.addAll(val.feed(stream));
            }
            return out;
        }
        // we have operations -- there's an optimization here where some operations could be simply added
        // to the merged streams and then we could feed directly; for now we'll just materialize this stream and then feed
        OperatorValue tgt = OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_CREATE, this.stream));
        List<OperatorValue> steps = Lists.newArrayList();
        for (StreamValue val : inputStreams) {
            steps.addAll(val.feed(tgt));
        }
        OperatorNode<PhysicalExprOperator> complete = OperatorNode.create(PhysicalExprOperator.STREAM_COMPLETE, stream, steps);

        return ImmutableList.of(
                OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EXECUTE, OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, complete,
                        setSink(this.stream, OperatorNode.create(SinkOperator.STREAM,
                                OperatorNode.create(PhysicalExprOperator.VALUE, stream))))));
    }

    @Override
    public Collection<OperatorValue> feedIf(OperatorNode<PhysicalExprOperator> condition, OperatorValue stream) {
        if (this.stream.getOperator() == StreamOperator.SINK) {
            // no operations tied to this stream -- feed our children directly into that stream
            List<OperatorValue> out = Lists.newArrayList();
            for (StreamValue val : inputStreams) {
                out.addAll(val.feedIf(condition, stream));
            }
            return out;
        }
        // we have operations -- there's an optimization here where some operations could be simply added
        // to the merged streams and then we could feed directly; for now we'll just materialize this stream and then feed
        OperatorValue tgt = OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, context.getContextExpr(),
                OperatorNode.create(PhysicalExprOperator.STREAM_CREATE, this.stream));
        List<OperatorValue> steps = Lists.newArrayList();
        for (StreamValue val : inputStreams) {
            steps.addAll(val.feedIf(condition, tgt));
        }
        OperatorNode<PhysicalExprOperator> complete = OperatorNode.create(PhysicalExprOperator.STREAM_COMPLETE, stream, steps);

        return ImmutableList.of(
                OperatorStep.create(context.getValueTypeAdapter(), PhysicalOperator.EVALUATE, OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, complete,
                        setSink(this.stream, OperatorNode.create(SinkOperator.STREAM,
                                OperatorNode.create(PhysicalExprOperator.VALUE, stream))))));
    }
}
