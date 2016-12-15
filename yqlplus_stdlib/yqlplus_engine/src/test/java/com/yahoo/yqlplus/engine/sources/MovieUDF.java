/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.Export;

import java.util.Arrays;
import java.util.List;

public class MovieUDF implements Exports {

    @Export
    public List<String> parseReviews(String input) {
        return Arrays.asList(input.split(" "));
    }
}
