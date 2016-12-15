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
 * Operators on expressions.
 */
public enum ExpressionOperator implements Operator {
    AND(TypeCheckers.EXPRS),
    OR(TypeCheckers.EXPRS),
    EQ(ExpressionOperator.class, ExpressionOperator.class),
    NEQ(ExpressionOperator.class, ExpressionOperator.class),
    LT(ExpressionOperator.class, ExpressionOperator.class),
    GT(ExpressionOperator.class, ExpressionOperator.class),
    LTEQ(ExpressionOperator.class, ExpressionOperator.class),
    GTEQ(ExpressionOperator.class, ExpressionOperator.class),

    IN(ExpressionOperator.class, ExpressionOperator.class),
    IN_QUERY(ExpressionOperator.class, SequenceOperator.class),
    NOT_IN(ExpressionOperator.class, ExpressionOperator.class),
    NOT_IN_QUERY(ExpressionOperator.class, SequenceOperator.class),

    LIKE(ExpressionOperator.class, ExpressionOperator.class),
    NOT_LIKE(ExpressionOperator.class, ExpressionOperator.class),

    IS_NULL(ExpressionOperator.class),
    IS_NOT_NULL(ExpressionOperator.class),
    MATCHES(ExpressionOperator.class, ExpressionOperator.class),
    NOT_MATCHES(ExpressionOperator.class, ExpressionOperator.class),
    CONTAINS(ExpressionOperator.class, ExpressionOperator.class),

    ADD(ExpressionOperator.class, ExpressionOperator.class),
    SUB(ExpressionOperator.class, ExpressionOperator.class),
    MULT(ExpressionOperator.class, ExpressionOperator.class),
    DIV(ExpressionOperator.class, ExpressionOperator.class),
    MOD(ExpressionOperator.class, ExpressionOperator.class),

    NEGATE(ExpressionOperator.class),
    NOT(ExpressionOperator.class),

    MAP(TypeCheckers.LIST_OF_STRING, TypeCheckers.EXPRS),

    ARRAY(TypeCheckers.EXPRS),

    INDEX(ExpressionOperator.class, ExpressionOperator.class),
    PROPREF(ExpressionOperator.class, String.class),

    CALL(TypeCheckers.LIST_OF_STRING, TypeCheckers.EXPRS),

    VARREF(String.class),

    LITERAL(TypeCheckers.LITERAL_TYPES),

    READ_RECORD(String.class),
    READ_FIELD(String.class, String.class),
    READ_MODULE(TypeCheckers.LIST_OF_STRING),

    NULL();

    private final ArgumentsTypeChecker checker;


    private ExpressionOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }

    public static Predicate<OperatorNode<? extends Operator>> IS = new Predicate<OperatorNode<? extends Operator>>() {
        @Override
        public boolean apply(OperatorNode<? extends Operator> input) {
            return input.getOperator() instanceof ExpressionOperator;
        }
    };
}
