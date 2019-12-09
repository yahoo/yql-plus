/*
 * Copyright (c) 2019 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class SampleExecutionSource implements Source {
    @Query
    public Sample selectResult(final String id) {
        Sample sample = new Sample();
        sample.start = System.currentTimeMillis();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        sample.id = id;
        sample.end = System.currentTimeMillis();
        return sample;
    }

    public static class Sample {
        public String id;
        public long start;
        public long end;
    }
}
