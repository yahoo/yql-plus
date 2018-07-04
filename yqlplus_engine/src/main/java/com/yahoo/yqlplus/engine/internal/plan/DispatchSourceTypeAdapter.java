/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.StreamValue;

import java.util.List;
import java.util.Map;

public class DispatchSourceTypeAdapter implements SourceType {
    private Map<Integer, SourceType> handlers;

    public DispatchSourceTypeAdapter(Map<Integer, SourceType> handlers) {
        this.handlers = handlers;
    }

    @Override
    public StreamValue plan(ContextPlanner planner, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source) {
        // dispatch arguments
        SourceType target = dispatch(source);
        return target.plan(planner, query, source);
    }

    private SourceType dispatch(OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        int count = args.size();
        SourceType target = handlers.get(count);
        if (target == null) {
            throw new ProgramCompileException("Unable to match arguments %s to source %s", args, source.getArgument(0));
        }
        return target;
    }

    @Override
    public StreamValue join(ContextPlanner planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source) {
        SourceType target = dispatch(source);
        return target.join(planner, leftSide, joinExpression, query, source);
    }
}
