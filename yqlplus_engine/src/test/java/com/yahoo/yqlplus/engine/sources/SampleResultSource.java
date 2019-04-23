/*
 * Copyright (c) 2019 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.Arrays;
import java.util.List;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class SampleResultSource implements Source {
    
    private Sample field1 = new Sample();
    
    private Sample field2 = new Result<Sample>(field1).getResult();

    @Query
    public List<Result<Sample>> selectResult() {
        Sample sample = new Sample();
        sample.id = "id";
        return Arrays.asList(new Result<Sample>(sample), new Result<Sample>(field2));
    }
    
    public static class Sample {
        public String id = "id";
    }

    public static class Result<T> {
        private T result;

        public Result(T result) {
            this.result = result;
        }

        public T getResult() {
            return result;
        }
    }
}
