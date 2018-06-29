/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Predicate;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PlanOperatorTypes;
import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public enum TaskOperator implements Operator {
    ARGUMENT(String.class, YQLType.class),

    // start, arguments, tasks
    PLAN(TaskOperator.class, new TypeLiteral<List<OperatorNode<TaskOperator>>>() {
    }, new TypeLiteral<List<OperatorNode<TaskOperator>>>() {
    }),

    // invocations
    // invoke a RUN task
    CALL(String.class, PlanOperatorTypes.VALUES),

    // invoke a JOIN task
    READY(String.class, PlanOperatorTypes.VALUES),


    // invoke multiple tasks
    PARALLEL(new TypeLiteral<List<OperatorNode<TaskOperator>>>() {
    }),
    // END!
    END(),

    // --- task types

    // execute some steps
    // RUN(inputs, steps, next)
    RUN(String.class, PlanOperatorTypes.VALUES, new TypeLiteral<List<OperatorStep>>() {
    }, TaskOperator.class),

    // a JOIN point
    JOIN(String.class, PlanOperatorTypes.VALUES, TaskOperator.class, Integer.class);


    private final ArgumentsTypeChecker checker;

    TaskOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }

}
