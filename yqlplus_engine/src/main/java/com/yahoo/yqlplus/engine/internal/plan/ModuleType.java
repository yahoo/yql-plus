/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.StreamValue;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;

public interface ModuleType {
    OperatorNode<PhysicalExprOperator> call(Location location, ContextPlanner context, String name, List<OperatorNode<ExpressionOperator>> arguments);

    OperatorNode<PhysicalExprOperator> callInRowContext(Location location, ContextPlanner context, String name, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row);

    OperatorNode<PhysicalExprOperator> property(Location location, ContextPlanner context, String name);

    StreamValue pipe(Location location, ContextPlanner context, String name, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments);
}
