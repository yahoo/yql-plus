/*
 * Copyright (c) 2019 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class GenericFieldResultSource implements Source {
    @Query
    public Result selectResult() {
        Sample sample = new Sample();
        sample.id = "id";
        Result result = new Result<Sample>();
        result.myField = sample;
        return result;
    }
    
    public static class Sample {
        public String id = "id";
    }

    public static class Result<T> {
        public T myField;
    }
}
