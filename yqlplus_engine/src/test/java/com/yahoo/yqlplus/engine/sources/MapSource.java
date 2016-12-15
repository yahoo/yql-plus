/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

import java.util.Map;

public class MapSource implements Source {

    @Query
    public SampleId getSampleId(@Key("id") Integer id, Map<String, String> map) {
        return new SampleId(id + map.size());
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
