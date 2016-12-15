/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.yahoo.yqlplus.engine.YQLResultSet;

public class PlanResultSet implements YQLResultSet {
    private final Object result;

    public PlanResultSet(Object result) {
        this.result = result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getResult() {
        // this was a poor choice of API -- YQLResultSet is a pointless indirection, but changing it
        // means refactoring all the callers for little benefit.
        return (T) result;
    }
}
