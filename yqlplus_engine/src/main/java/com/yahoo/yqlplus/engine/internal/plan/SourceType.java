/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

public interface SourceType {
    StreamValue plan(ContextPlanner planner, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source);
    StreamValue join(ContextPlanner planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> right, OperatorNode<SequenceOperator> source);
}
