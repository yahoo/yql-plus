/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.HashSet;
import java.util.Set;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class SampleListSource implements Source {

    @Query
    public Iterable<Sample> getSampleIds() {

        Set<Sample> ids = new HashSet<>();
        for (int i = 0; i < 1; i++) {
            ids.add(new Sample("test", i));
        }
        return ids;
    }
    
    @Query
    public Iterable<Sample> getSampleIds(Iterable<Sample> ids) {
      return ids;
    }
}
