/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class NullIterableSource implements Source {

    @Query
    public Iterable<SampleId> getSampleIds(@Key("id") Integer id) {
        return null;
    }

    public static class SampleId {

        private final int id;

        public SampleId(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
}

