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

public class BoxedParameterSource implements Source {

    @Query
    public Iterable<Sample> getSampleIds(Byte b, Short s, Integer i, Long l, Double d, Boolean bb) {
        List<Sample> samples = Lists.newArrayList();
        samples.add(new Sample(b, s, i, l, d, bb));
        return samples;
    }

    public static class Sample {

        private final byte b;
        private final short s;
        private final int i;
        private final long l;
        private final double d;
        private final boolean bb;
        
        public Sample(byte b, short s, int i, long l, double d, boolean bb) {
            super();
            this.b = b;
            this.s = s;
            this.i = i;
            this.l = l;
            this.d = d;
            this.bb = bb;
        }

        public byte getB() {
            return b;
        }
        public short getS() {
            return s;
        }
        public int getI() {
            return i;
        }
        public long getL() {
            return l;
        }
        public double getD() {
            return d;
        }
        public boolean isBb() {
            return bb;
        }      
    }
}
