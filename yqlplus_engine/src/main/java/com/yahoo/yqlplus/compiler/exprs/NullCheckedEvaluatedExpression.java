/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.exprs;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.types.NotNullableTypeWidget;

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
