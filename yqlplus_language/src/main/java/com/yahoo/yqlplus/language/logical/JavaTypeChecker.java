/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.language.operator.Operator;

public class JavaTypeChecker extends OperatorTypeChecker {
    private final Class<?> type;

    public JavaTypeChecker(Operator parent, int idx, Class<?> type) {
        super(parent, idx);
        this.type = type;
    }

    @Override
    public void check(Object argument) {
        Preconditions.checkNotNull(argument, "Argument %s of %s must not be null", idx, parent);
        Preconditions.checkArgument(type.isInstance(argument), "Argument %s of %s must be %s (is: %s).", idx, parent, type.getName(), argument.getClass().getName());
    }

}
