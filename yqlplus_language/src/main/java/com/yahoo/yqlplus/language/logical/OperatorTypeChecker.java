/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.yahoo.yqlplus.language.operator.Operator;

/**
 * Check the type of a single argument.
 */
public abstract class OperatorTypeChecker {
    protected final Operator parent;
    protected final int idx;

    protected OperatorTypeChecker(Operator parent, int idx) {
        this.parent = parent;
        this.idx = idx;
    }

    public abstract void check(Object argument);
}
