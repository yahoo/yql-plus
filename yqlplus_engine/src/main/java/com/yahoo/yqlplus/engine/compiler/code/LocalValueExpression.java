/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public abstract class LocalValueExpression extends BaseTypeExpression implements EvaluatedExpression {
    public LocalValueExpression(TypeWidget type) {
        super(type);
    }
}
