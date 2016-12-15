/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.tasks;

import java.util.Set;

public interface Step {
    Set<? extends Value> getInputs();

    Value getOutput();

    boolean isAsync();
}
