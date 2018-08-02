/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;

import java.util.List;

public interface DynamicExpressionEnvironment {
    OperatorValue getVariable(String name);

    OperatorNode<PhysicalExprOperator> call(OperatorNode<ExpressionOperator> call);

    OperatorNode<PhysicalExprOperator> call(OperatorNode<ExpressionOperator> call, OperatorNode<PhysicalExprOperator> row);

    OperatorNode<PhysicalExprOperator> evaluate(OperatorNode<ExpressionOperator> value);

    OperatorNode<PhysicalExprOperator> property(Location location, List<String> path);

    OperatorNode<PhysicalExprOperator> constant(Object value);
}
