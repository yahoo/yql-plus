/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;


import com.yahoo.yqlplus.engine.sources.MapSyntaxTestSource.MapSyntaxTestRecord;

public class NestedMapSource implements Source {
    @Query
    public NestedMapRecord scan() {
        return new NestedMapRecord();
    }
    

    public static class NestedMapRecord {
        public MapSyntaxTestRecord result = new MapSyntaxTestRecord();
    }
}