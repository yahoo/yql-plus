/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.streams;

import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;


public enum StreamOperator implements Operator {
    SINK(SinkOperator.class),

    // stream operations
    DISTINCT(StreamOperator.class),
    // row -> *row
    FLATTEN(StreamOperator.class),
    FILTER(StreamOperator.class, FunctionOperator.class),
    OFFSET(StreamOperator.class, PhysicalExprOperator.class),
    LIMIT(StreamOperator.class, PhysicalExprOperator.class),
    SLICE(StreamOperator.class, PhysicalExprOperator.class, PhysicalExprOperator.class),
    ORDERBY(StreamOperator.class, FunctionOperator.class),
    TRANSFORM(StreamOperator.class, FunctionOperator.class),
    // like transform but parallel
    SCATTER(StreamOperator.class, FunctionOperator.class),
    // GROUPBY((row) -> key, (key, rows) -> row)
    GROUPBY(StreamOperator.class, FunctionOperator.class, FunctionOperator.class),
    // CROSS(right_rows, (left, right) -> rows)
    CROSS(StreamOperator.class, PhysicalExprOperator.class, FunctionOperator.class),
    // HASH_JOIN(right_sequence, (left) -> key, (right) -> key, (left, right) -> row)
    HASH_JOIN(StreamOperator.class, PhysicalExprOperator.class, FunctionOperator.class, FunctionOperator.class, FunctionOperator.class),
    // OUTER_HASH_JOIN(right_sequence, (left) -> key, (right) -> key, (left, right_or_null) -> row)
    OUTER_HASH_JOIN(StreamOperator.class, PhysicalExprOperator.class, FunctionOperator.class, FunctionOperator.class, FunctionOperator.class);

    public static OperatorNode<StreamOperator> create(Location loc, StreamOperator operator, Object... arguments) {
        return OperatorNode.create(loc, operator, arguments);
    }

    private final ArgumentsTypeChecker checker;

    StreamOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }
}
