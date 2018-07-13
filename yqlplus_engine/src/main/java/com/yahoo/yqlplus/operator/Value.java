/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

/**
 * Represents the output of a Step. Keeps track of its source and dependencies.
 */
public interface Value {
    String getName();

    Step getSource();
}
