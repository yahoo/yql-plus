/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.yahoo.yqlplus.language.operator.Operator;

import java.util.Set;

public class JavaUnionTypeChecker extends OperatorTypeChecker {
    private final Set<Class<?>> types;

    public JavaUnionTypeChecker(Operator parent, int idx, Set<Class<?>> types) {
        super(parent, idx);
        this.types = types;
    }

    public JavaUnionTypeChecker(Operator parent, int idx, Class<?>... types) {
        super(parent, idx);
        this.types = ImmutableSet.copyOf(types);
    }

    @Override
    public void check(Object argument) {
        Preconditions.checkNotNull(argument, "Argument %s of %s must not be null", idx, parent);
        for (Class<?> candidate : types) {
            if (candidate.isInstance(argument)) {
                return;
            }
        }
        Preconditions.checkArgument(false, "Argument %s of %s must be %s (is: %s).", idx, parent, Joiner.on("|").join(types), argument.getClass());
    }


}
