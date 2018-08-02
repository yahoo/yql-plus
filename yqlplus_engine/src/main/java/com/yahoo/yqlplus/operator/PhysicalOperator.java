/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.compiler.code.EngineValueTypeAdapter;
import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

public enum PhysicalOperator implements Operator {
    REQUIRED_ARGUMENT(String.class, YQLType.class),
    OPTIONAL_ARGUMENT(String.class, YQLType.class, Object.class),

    OUTPUT(String.class, PhysicalExprOperator.class),
    END(PlanOperatorTypes.VALUES),

    // context
    EVALUATE(PhysicalExprOperator.class, PhysicalExprOperator.class),
    // execute evalutes and expression and throws away the result
    EXECUTE(PhysicalExprOperator.class, PhysicalExprOperator.class),
    // evaluate_guard enforces the timeout on the computation
    EVALUATE_GUARD(PhysicalExprOperator.class, PhysicalExprOperator.class);

    public static OperatorNode<PhysicalOperator> create(PhysicalOperator operator, Object... arguments) {
        return OperatorNode.create(operator, arguments);
    }

    private final ArgumentsTypeChecker checker;

    PhysicalOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }


    public boolean hasResult(EngineValueTypeAdapter typeAdapter, OperatorNode<PhysicalOperator> op) {
        switch (op.getOperator()) {
            case OUTPUT:
            case END:
                return false;
            case REQUIRED_ARGUMENT:
            case OPTIONAL_ARGUMENT: {
                return true;
            }
            case EVALUATE_GUARD:
            case EVALUATE: {
                OperatorNode<PhysicalExprOperator> expr = op.getArgument(1);
                return true;
            }
            case EXECUTE:
                return false;
            default:
                throw new IllegalArgumentException("unknown PhysicalOperator: " + op);
        }
    }
}
