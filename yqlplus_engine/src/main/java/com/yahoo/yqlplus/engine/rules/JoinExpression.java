/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.List;

public final class JoinExpression {
    public final OperatorNode<ExpressionOperator> left;
    // today this has to be EQ
    public final OperatorNode<ExpressionOperator> right;

    JoinExpression(OperatorNode<ExpressionOperator> left, OperatorNode<ExpressionOperator> right) {
        this.left = left;
        this.right = right;
    }

    public String getRightField() {
        Preconditions.checkState(right.getOperator() == ExpressionOperator.READ_FIELD);
        return right.getArgument(1);
    }

    public static List<JoinExpression> parse(OperatorNode<ExpressionOperator> expr) {
        List<JoinExpression> joinColumns = Lists.newArrayList();
        parseJoinExpression(joinColumns, expr);
        return joinColumns;
    }

    /**
     * Relies on NormalizeJoinExpression having run.
     *
     * @param columns
     * @param expr
     * @see com.yahoo.yqlplus.engine.rules.NormalizeJoinExpression
     */
    private static void parseJoinExpression(List<JoinExpression> columns, OperatorNode<ExpressionOperator> expr) {
        switch (expr.getOperator()) {
            case AND: {
                List<OperatorNode<ExpressionOperator>> clauses = expr.getArgument(0);
                for (OperatorNode<ExpressionOperator> clause : clauses) {
                    parseJoinExpression(columns, clause);
                }
                break;
            }
            case EQ: {
                OperatorNode<ExpressionOperator> left = expr.getArgument(0);
                OperatorNode<ExpressionOperator> right = expr.getArgument(1);
                columns.add(new JoinExpression(left, right));
                break;
            }
            default:
                throw new ProgramCompileException(expr.getLocation(), "Unsupported JOIN expression clause type: " + expr.getOperator());
        }
    }

}
