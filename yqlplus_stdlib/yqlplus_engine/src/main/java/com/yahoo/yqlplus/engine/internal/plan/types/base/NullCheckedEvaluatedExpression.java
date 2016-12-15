/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.EvaluatedExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public class NullCheckedEvaluatedExpression implements BytecodeExpression, EvaluatedExpression {
    private final BytecodeExpression input;
    private final TypeWidget checkedType;

    public NullCheckedEvaluatedExpression(BytecodeExpression evaluatedChecked) {
        Preconditions.checkArgument(evaluatedChecked instanceof EvaluatedExpression);
        Preconditions.checkArgument(evaluatedChecked.getType().isNullable());
        this.input = evaluatedChecked;
        this.checkedType = NotNullableTypeWidget.create(evaluatedChecked.getType());
    }

    @Override
    public TypeWidget getType() {
        return checkedType;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.exec(input);
    }
}
