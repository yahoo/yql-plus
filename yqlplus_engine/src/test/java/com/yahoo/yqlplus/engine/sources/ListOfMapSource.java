/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ListOfMapSource implements Source {

    @Query
    public List<Map<String, String>> makeObject(String key, Collection<String> values) {
        List<Map<String, String>> objs = new ArrayList<>(values.size());
        for (String value : values) {
            Map<String, String> obj = ImmutableMap.of(key, value);
            objs.add(obj);
        }
        return objs;
    }
}

