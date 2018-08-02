/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Push AND operators THROUGH OR operators by expansion.
 */
public class PushAndTransform extends LogicalOperatorTransform {
    @Override
    public OperatorNode<ExpressionOperator> visitExpr(OperatorNode<ExpressionOperator> expr) {
        if (expr.getOperator() == ExpressionOperator.AND) {
            // (AND a b c (OR d x))
            //  -> (OR (AND a b c d) (AND a b c x))
            // a AND (b OR c) AND (d OR x)
            //  -> (a AND b AND d) OR (a AND c AND x)
            List<OperatorNode<ExpressionOperator>> args = expr.getArgument(0);
            if (args.size() == 1) {
                return visitExpr(args.get(0));
            }
            final List<OperatorNode<ExpressionOperator>> ands = Lists.newArrayList();
            List<List<OperatorNode<ExpressionOperator>>> ors = Lists.newArrayList();
            for (OperatorNode<ExpressionOperator> arg : args) {
                arg = visitExpr(arg);
                if (arg.getOperator() == ExpressionOperator.OR) {
                    ors.add(arg.getArgument(0));
                } else {
                    ands.add(arg);
                }
            }
            if (ors.isEmpty()) {
                // nothing to do
                return expr;
            }
            final List<OperatorNode<ExpressionOperator>> result = Lists.newArrayListWithExpectedSize(ors.size());
            // for every combination of OR clauses, emit a new AND clause
            enumerate(ors, new Visit() {
                @Override
                public void add(List<OperatorNode<ExpressionOperator>> clauses) {
                    List<OperatorNode<ExpressionOperator>> p = Lists.newArrayListWithExpectedSize(ands.size() + clauses.size());
                    p.addAll(ands);
                    p.addAll(clauses);
                    result.add(OperatorNode.create(ExpressionOperator.AND, p));
                }
            });
            if (result.size() == 1) {
                return result.get(0);
            } else {
                return OperatorNode.create(ExpressionOperator.OR, result);
            }
        }
        return super.visitExpr(expr);
    }

    interface Visit {
        void add(List<OperatorNode<ExpressionOperator>> clauses);
    }

    private void enumerate(List<List<OperatorNode<ExpressionOperator>>> input, Visit visit) {
        List<OperatorNode<ExpressionOperator>> partial = Lists.newArrayListWithCapacity(input.size());
        enumerate(partial, input, visit);
    }

    private void enumerate(List<OperatorNode<ExpressionOperator>> partial, List<List<OperatorNode<ExpressionOperator>>> input, Visit visit) {
        if (partial.size() == input.size()) {
            visit.add(partial);
        } else {
            int currentIndex = partial.size();
            for (OperatorNode<ExpressionOperator> clause : input.get(currentIndex)) {
                partial.add(clause);
                enumerate(partial, input, visit);
                partial.remove(clause);
            }
        }
    }
}
