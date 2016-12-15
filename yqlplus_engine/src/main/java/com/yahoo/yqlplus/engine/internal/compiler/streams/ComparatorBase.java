/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler.streams;

import java.util.Comparator;

public abstract class ComparatorBase implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return -1;
        } else if (o2 == null) {
            return 1;
        }
        return compareNotNull(o1, o2);
    }

    protected abstract int compareNotNull(Object o1, Object o2);
}
