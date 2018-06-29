/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.Export;

public class StringUtilUDF implements Exports {

    @Export
    public String toUpperCase(String input) {
        return input.toUpperCase();
    }
    
    @Export
    public boolean compareStr(String s1, String s2) {
        if (s1 != null) {
            return s1.equals(s2);
        } else {
            return s2 == null;
        }
    }
}
