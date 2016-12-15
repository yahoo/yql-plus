/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

import org.testng.collections.Lists;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class IntSource implements Source {

    @Query
    public Iterable<SampleId> getSampleIds(@Key("id") Integer id) {
        List<SampleId> ids = Lists.newArrayList();
        ids.add(new SampleId(id));
        return ids;
    }

    public static class SampleId {

        private final Integer id;

        public SampleId(Integer id) {
            this.id = id;
        }
        public Integer getId() {
            return id;
        }
    }
}