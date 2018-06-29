/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Preconditions;
import org.objectweb.asm.Label;

public class NullTestingComparisonAdapter implements ComparisonAdapter {
    private final TypeWidget nullableTarget;
    private final ComparisonAdapter target;

    public NullTestingComparisonAdapter(TypeWidget nullableTarget, TypeWidget target) {
        Preconditions.checkArgument(nullableTarget.isNullable());
        Preconditions.checkArgument(!target.isNullable());
        this.nullableTarget = nullableTarget;
        this.target = target.getComparisionAdapter();
    }

    @Override
    public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
        scope.nullTest(nullableTarget, isNull);
        target.coerceBoolean(scope, isTrue, isFalse, isNull);
    }
}
