/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Find references to named views, expand them with a copy of the operator tree for the view.
 */
public class ExpandViews extends LogicalOperatorTransform {
    private ViewRegistry scope;

    public ExpandViews(ViewRegistry scope) {
        this.scope = scope;
    }

    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        // a view reference has to be in a 'SCAN'
        if (node.getOperator() != SequenceOperator.SCAN) {
            return super.visitSequenceOperator(node);
        }
        List<String> name = (List<String>) node.getArgument(0);
        List<OperatorNode<ExpressionOperator>> arguments = (List<OperatorNode<ExpressionOperator>>) node.getArgument(1);
        // a view reference has to have no arguments
        if (arguments.size() > 0) {
            // we need to continue traversing because the arguments are nodes too
            return super.visitSequenceOperator(node);
        }
        OperatorNode<SequenceOperator> view = scope.getView(name);
        if (view == null) {
            return super.visitSequenceOperator(node);
        }
        // OK, so we have a SCAN node and we know it maps to a view.
        // Make a deep copy of the view so later transforms can mutate it safely.
        view = view.copy();
        String alias = (String) node.getAnnotation("alias");
        if (alias != null) {
            view.putAnnotation("alias", alias);
        }
        return view;
    }
}
