/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class SampleListSourceWithBoxedParams implements Source {

    /**
     * select * from SampleList where category = @category
     * 
     * @param category
     * @param count
     * @return
     * @throws InterruptedException 
     */
    @Query
    public Iterable<SampleId> getSampleIds(@Key("category") String category,
            Integer count, Double doubleValue, String stringValue) {

        List<SampleId> ids = new ArrayList<SampleId>(count);
        for (int i = 0; i < count; i++) {
            ids.add(new SampleId(category, i));
        }
        return ids;
    }

    public class SampleId {
        private String category;
        private int id;

        public SampleId(String category, int id) {
            this.category = category;
            this.id = id;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
}
