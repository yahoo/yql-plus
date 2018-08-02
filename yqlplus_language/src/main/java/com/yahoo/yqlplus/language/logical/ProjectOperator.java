/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Predicate;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

/**
 * Represents a projection command which affects the output record.
 */
public enum ProjectOperator implements Operator {
    FIELD(ExpressionOperator.class, String.class),  // FIELD expr name
    MERGE_RECORD(String.class);                     // MERGE_RECORD name (alias of record to merge)

    private final ArgumentsTypeChecker checker;

    public static Predicate<OperatorNode<? extends Operator>> IS = new Predicate<OperatorNode<? extends Operator>>() {
        @Override
        public boolean apply(OperatorNode<? extends Operator> input) {
            return input.getOperator() instanceof ProjectOperator;
        }
    };

    ProjectOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }

}
