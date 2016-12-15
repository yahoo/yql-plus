/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class FRSource implements Source {
    @Query
    public FloatRecord scan() {
        return new FloatRecord(1.0f, 2.0);
    }

    @Query
    public FloatRecord make(double a, double b) {
        return new FloatRecord((float) a, b);
    }
    
    public static class FloatRecord {
        public float floaty;
        public double doubley;

        public FloatRecord(float floaty, double doubley) {
            this.floaty = floaty;
            this.doubley = doubley;
        }
    }
}

