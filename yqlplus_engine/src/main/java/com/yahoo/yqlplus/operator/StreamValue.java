/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.Collection;
import java.util.List;

public abstract class StreamValue {
    public static StreamValue merge(ContextPlanner planner, List<StreamValue> inputStreams) {
        return new MergeStreamValue(planner, inputStreams);
    }

    public static StreamValue singleton(ContextPlanner context, OperatorNode<PhysicalExprOperator> input) {
        return new SourceStreamValue(context, OperatorNode.create(input.getLocation(), PhysicalExprOperator.SINGLETON, input));
    }

    public static StreamValue iterate(ContextPlanner context, OperatorNode<PhysicalExprOperator> input) {
        return new SourceStreamValue(context, input);
    }

    public static StreamValue iterate(ContextPlanner context, OperatorValue input) {
        return iterate(context, OperatorNode.create(PhysicalExprOperator.VALUE, input));
    }

    protected ContextPlanner context;
    OperatorNode<StreamOperator> stream;

    protected StreamValue(ContextPlanner context) {
        this.context = context;
        this.stream = OperatorNode.create(StreamOperator.SINK, OperatorNode.create(SinkOperator.ACCUMULATE));
    }

    public void add(Location location, StreamOperator operator, Object... arguments) {
        // this is a little expensive; maybe revisit how we build this up
        this.stream = setTail(this.stream, location, operator, arguments);
    }

    public static OperatorNode<StreamOperator> setSink(OperatorNode<StreamOperator> target, OperatorNode<SinkOperator> sink) {
        return setTail(target, sink.getLocation(), StreamOperator.SINK, sink);
    }

    public static OperatorNode<StreamOperator> setTail(OperatorNode<StreamOperator> target, Location location, StreamOperator operator, Object... arguments) {
        switch (target.getOperator()) {
            case SINK: {
                if (operator == StreamOperator.SINK) {
                    return OperatorNode.createAs(location, ImmutableMap.of(), operator, arguments);
                } else {
                    Object[] args = new Object[arguments == null ? 1 : 1 + arguments.length];
                    args[0] = target;
                    if (arguments != null) {
                        System.arraycopy(arguments, 0, args, 1, arguments.length);
                    }
                    return OperatorNode.createAs(location, ImmutableMap.of(), operator, args);
                }
            }
            case DISTINCT:
            case FLATTEN:
            case FILTER:
            case OFFSET:
            case LIMIT:
            case SLICE:
            case ORDERBY:
            case HASH_JOIN:
            case OUTER_HASH_JOIN:
            case TRANSFORM:
            case SCATTER:
            case GROUPBY: {
                Object[] oldArguments = target.getArguments();
                Object[] newArguments = new Object[oldArguments.length];
                newArguments[0] = setTail((OperatorNode<StreamOperator>) oldArguments[0], location, operator, arguments);
                System.arraycopy(oldArguments, 1, newArguments, 1, oldArguments.length - 1);
                return OperatorNode.createAs(target.getLocation(), target.getAnnotations(), target.getOperator(), newArguments);
            }
            default:
                throw new UnsupportedOperationException("Unknown stream operator: " + target);
        }
    }

    public abstract OperatorValue materialize();

    public abstract OperatorValue  materializeIf(OperatorNode<PhysicalExprOperator> condition);

    public abstract Collection<OperatorValue> feed(OperatorValue stream);

    public abstract Collection<OperatorValue> feedIf(OperatorNode<PhysicalExprOperator> condition, OperatorValue stream);

    public final OperatorNode<PhysicalExprOperator> materializeValue() {
        return OperatorNode.create(PhysicalExprOperator.VALUE, materialize());
    }
}
