/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.sources.ArraySyntaxTestSource.ArraySyntaxTestRecord;

public class NestedSource implements Source {
    @Query
    public NestedArrayRecord scan() {
        return new NestedArrayRecord();
    }

    public static class NestedArrayRecord {
        public ArraySyntaxTestRecord result = new ArraySyntaxTestRecord();
    }
}
