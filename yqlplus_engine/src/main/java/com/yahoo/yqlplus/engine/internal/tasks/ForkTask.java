/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.tasks;

import com.google.common.collect.Sets;
import com.yahoo.yqlplus.operator.Value;

import java.util.Set;

public class ForkTask extends Task {
    @Override
    public Set<Value> getInputs() {
        Set<Value> values = Sets.newIdentityHashSet();
        for (Task next : getNext()) {
            values.addAll(next.getInputs());
        }
        return values;
    }
}
