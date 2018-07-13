/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.indexed;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class QueryStrategy {
    public boolean scan = false;
    public Multimap<IndexKey, IndexStrategy> indexes = ArrayListMultimap.create();

    public void add(IndexStrategy indexStrategy) {
        indexes.put(indexStrategy.index, indexStrategy);
    }
}
