/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.ast;

import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;


public enum SinkOperator implements Operator {
    ACCUMULATE(),
    STREAM(PhysicalExprOperator.class);

    public static OperatorNode<SinkOperator> create(Location loc, SinkOperator operator, Object... arguments) {
        return OperatorNode.create(loc, operator, arguments);
    }

    private final ArgumentsTypeChecker checker;

    SinkOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }
}
