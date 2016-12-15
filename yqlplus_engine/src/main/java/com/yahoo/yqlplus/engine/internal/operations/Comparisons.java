/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.operations;

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.regex.Pattern;

public final class Comparisons {
    public static final Comparisons INSTANCE = new Comparisons();

    private Comparisons() {

    }

    public <T> int compare(Comparable<T> left, T right) {
        if (left == null && right == null) {
            return 0;
        } else if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        }
        return left.compareTo(right);
    }

    public boolean compare(BinaryComparison op, int left, int right) {
        Preconditions.checkNotNull(left);
        Preconditions.checkNotNull(right);
        int val = Integer.compare(left, right);
        switch (op) {
            case LT:
                return val < 0;
            case LTEQ:
                return val <= 0;
            case GT:
                return val > 0;
            case GTEQ:
                return val >= 0;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public boolean compare(BinaryComparison op, Comparable left, Comparable right) {
        int val = left.compareTo(right);
        switch (op) {
            case LT:
                return val < 0;
            case LTEQ:
                return val <= 0;
            case GT:
                return val > 0;
            case GTEQ:
                return val >= 0;
        }
        throw new UnsupportedOperationException("Unsupported comparison " + left + " " + op + " " + right);
    }

    public boolean matches(BinaryComparison op, CharSequence value, Pattern matches) {
        if (value == null || matches == null) {
            return false;
        }
        return matches.matcher(value).matches();
    }

    public boolean in(BinaryComparison op, Object value, Collection matches) {
        if (value == null || matches == null) {
            return false;
        }
        return matches.contains(value);
    }
}
