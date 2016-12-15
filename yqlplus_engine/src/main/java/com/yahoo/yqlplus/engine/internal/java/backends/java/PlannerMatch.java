/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.backends.java;

import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public class PlannerMatch implements Comparable<PlannerMatch> {
    // lower is better -- we pick the lowest scored match
    int priority;
    // the matching planner
    Planner planner;
    // the filter that should be applied to the results (may be modified by the planner)
    OperatorNode<ExpressionOperator> filter;
    // arguments to the function invocation (e.g. SELECT * FROM foo(1, 2) -> scanArguments of 1, 2
    List<OperatorNode<ExpressionOperator>> scanArguments;
    // For keyed planners, the union of the values of key expressions + all sequence expressions (which should be sequences of keys)
    // will define the set of values to invoke the source
    List<OperatorNode<ExpressionOperator>> keyExpressions;
    List<OperatorNode<ExpressionOperator>> keySequenceExpressions;

    public PlannerMatch(int priority, Planner planner, OperatorNode<ExpressionOperator> filter, List<OperatorNode<ExpressionOperator>> scanArguments) {
        this.priority = priority;
        this.planner = planner;
        this.filter = filter;
        this.scanArguments = scanArguments;
    }

    public PlannerMatch(int priority, Planner planner, OperatorNode<ExpressionOperator> filter, List<OperatorNode<ExpressionOperator>> scanArguments, List<OperatorNode<ExpressionOperator>> keyExpressions, List<OperatorNode<ExpressionOperator>> keySequenceExpressions) {
        this.priority = priority;
        this.planner = planner;
        this.filter = filter;
        this.scanArguments = scanArguments;
        this.keyExpressions = keyExpressions;
        this.keySequenceExpressions = keySequenceExpressions;
    }

    @Override
    public int compareTo(PlannerMatch o) {
        return Integer.compare(priority, o.priority);
    }
}
