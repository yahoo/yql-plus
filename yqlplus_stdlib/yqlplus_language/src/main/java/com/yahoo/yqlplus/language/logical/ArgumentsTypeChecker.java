/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.language.operator.Operator;

import java.util.List;

public final class ArgumentsTypeChecker {
    private final Operator target;
    private final List<OperatorTypeChecker> checkers;

    public ArgumentsTypeChecker(Operator target, List<OperatorTypeChecker> checkers) {
        this.target = target;
        this.checkers = checkers;
    }

    public void check(Object... args) {
        if (args == null) {
            Preconditions.checkArgument(checkers.size() == 0, "Operator %s argument count mismatch: expected %s got 0", target, checkers.size());
            return;
        } else {
            Preconditions.checkArgument(args.length == checkers.size(), "Operator %s argument count mismatch: expected: %s got %s", target, checkers.size(), args.length);
        }
        for (int i = 0; i < checkers.size(); ++i) {
            checkers.get(i).check(args[i]);
        }
    }

}
