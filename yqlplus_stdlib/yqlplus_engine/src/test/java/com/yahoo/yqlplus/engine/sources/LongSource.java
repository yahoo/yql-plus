/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;
import org.testng.collections.Lists;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class LongSource implements Source {

    @Query
    public Iterable<Sample> getSampleIds(Long start, Long end) {
        List<Sample> samples = Lists.newArrayList();
        samples.add(new Sample(start, end));
        return samples;
    }
    
    @Query
    public Iterable<Sample> getSampleIds(long start, long end, boolean increment) {
        List<Sample> samples = Lists.newArrayList();
        if (increment) {         
            samples.add(new Sample(start + 1, end + 1));
        } else {
            samples.add(new Sample(start, end));
        }
        return samples;
    }

    public static class Sample {

        private final long start;
        private final long end;

        public Sample(long start, long end) {
            this.start = start;
            this.end = end;
        }
        public long getStart() {
            return start;
        }
        public long getEnd() {
          return end;
      }
    }
}
