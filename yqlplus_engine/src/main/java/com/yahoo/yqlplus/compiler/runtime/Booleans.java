/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.runtime;

import java.util.Collection;
import java.util.Map;

public final class Booleans {
    private Booleans() {

    }

    public static boolean toBoolean(Object input) {
        if (input instanceof Boolean) {
            return ((Boolean) input);
        } else if (input instanceof Number) {
            return ((Number) input).intValue() != 0;
        } else if (input instanceof Collection) {
            return ((Collection) input).size() != 0;
        } else if (input instanceof Map) {
            return ((Map) input).size() != 0;
        } else {
            return true;
        }
    }
}
