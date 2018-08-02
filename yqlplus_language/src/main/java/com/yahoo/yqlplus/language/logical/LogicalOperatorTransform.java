/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Function;
import com.yahoo.yqlplus.language.operator.OperatorNode;


/**
 * Visit a logical OperatorNode tree, perhaps transforming each SequenceOperator node (deep, recursive)
 */
public abstract class LogicalOperatorTransform implements Function<Object, Object> {
    public OperatorNode<ExpressionOperator> visitExpr(OperatorNode<ExpressionOperator> expr) {
        return expr.transform(this);
    }

    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        return node.transform(this);
    }

    @Override
    public Object apply(Object input) {
        if (input instanceof OperatorNode) {
            OperatorNode<?> node = (OperatorNode) input;
            if (SequenceOperator.IS.apply(node)) {
                return visitSequenceOperator((OperatorNode<SequenceOperator>) node);
            } else if (ExpressionOperator.IS.apply(node)) {
                return visitExpr((OperatorNode<ExpressionOperator>) node);
            }
        }
        return input;
    }
}
