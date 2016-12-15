/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.ast;

import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public final class PlanOperatorTypes {
    public static final TypeLiteral<List<OperatorNode<StreamOperator>>> STREAM_OPERATORS = new TypeLiteral<List<OperatorNode<StreamOperator>>>() {
    };

    private PlanOperatorTypes() {
    }

    public static final TypeLiteral<List<OperatorNode<PhysicalExprOperator>>> EXPRS = new TypeLiteral<List<OperatorNode<PhysicalExprOperator>>>() {
    };

    public static final TypeLiteral<List<OperatorValue>> VALUES = new TypeLiteral<List<OperatorValue>>() {
    };
}
