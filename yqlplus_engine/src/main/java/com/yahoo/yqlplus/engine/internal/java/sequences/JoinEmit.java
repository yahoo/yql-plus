/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.sequences;

public interface JoinEmit<ROW, LEFT, RIGHT> {
    ROW create();

    void setLeft(ROW row, LEFT left);

    void setRight(ROW row, RIGHT right);
}
