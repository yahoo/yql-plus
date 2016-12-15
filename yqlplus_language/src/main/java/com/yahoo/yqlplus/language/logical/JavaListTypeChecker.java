/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.language.operator.Operator;

import java.util.List;

public class JavaListTypeChecker extends OperatorTypeChecker {
    private final Class<?> elementType;

    public JavaListTypeChecker(Operator parent, int idx, Class<?> elementType) {
        super(parent, idx);
        this.elementType = elementType;
    }

    @Override
    public void check(Object argument) {
        Preconditions.checkNotNull(argument, "Argument %s of %s must not be null", idx, parent);
        Preconditions.checkArgument(argument instanceof List, "Argument %s of %s must be a List<%s>", idx, parent, elementType.getName(), argument.getClass().getName());
        List<?> lst = (List<?>) argument;
        for (Object elt : lst) {
            Preconditions.checkNotNull(elt, "Argument %s of %s List elements may not be null", idx, parent);
            Preconditions.checkArgument(elementType.isInstance(elt), "Argument %s of %s List elements must be %s (is %s)", idx, parent, elementType.getName(), elt.getClass().getName());
        }
    }

}

