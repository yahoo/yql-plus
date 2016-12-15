/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Function;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import javax.annotation.Nullable;

/**
 * Visit a statement OperatorNode tree, perhaps transforming each StatementOperator node (deep, recursive)
 */
public abstract class StatementOperatorTransform implements Function<Object, Object> {
    public OperatorNode<ExpressionOperator> visitExpr(OperatorNode<ExpressionOperator> expr) {
        return expr.transform(this);
    }

    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        return node.transform(this);
    }

    public OperatorNode<StatementOperator> visitStatement(OperatorNode<StatementOperator> node) {
        return node.transform(this);
    }

    @Nullable
    @Override
    public Object apply(Object input) {
        if (input instanceof OperatorNode) {
            OperatorNode<?> node = (OperatorNode) input;
            if (SequenceOperator.IS.apply(node)) {
                return visitSequenceOperator((OperatorNode<SequenceOperator>) node);
            } else if (ExpressionOperator.IS.apply(node)) {
                return visitExpr((OperatorNode<ExpressionOperator>) node);
            } else if (StatementOperator.IS.apply(node)) {
                return visitStatement((OperatorNode<StatementOperator>) node);
            }
        }
        return input;
    }
}
