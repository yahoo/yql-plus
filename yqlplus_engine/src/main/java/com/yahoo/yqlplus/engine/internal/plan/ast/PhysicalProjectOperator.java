/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.ast;

import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;

public enum PhysicalProjectOperator implements Operator {
    FIELD(PhysicalExprOperator.class, String.class),  // FIELD expr name
    MERGE(PhysicalExprOperator.class);                // MERGE_RECORD name (alias of record to merge)


    private final ArgumentsTypeChecker checker;

    private PhysicalProjectOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }

}
