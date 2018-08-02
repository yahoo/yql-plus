/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class KeyedSource implements Source {
    @Query
    public List<IntegerKeyed> lookup(@Key("woeid") Integer integer) {
        return ImmutableList.of(new IntegerKeyed(integer));
    }
    
    public static class IntegerKeyed {
        public int woeid;

        public IntegerKeyed(int woeid) {
            this.woeid = woeid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntegerKeyed that = (IntegerKeyed) o;

            return woeid == that.woeid;
        }

        @Override
        public int hashCode() {
            return woeid;
        }
    }
}