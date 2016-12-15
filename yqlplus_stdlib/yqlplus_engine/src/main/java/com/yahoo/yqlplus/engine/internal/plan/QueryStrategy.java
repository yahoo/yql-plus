/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

class QueryStrategy {
    boolean scan = false;
    Multimap<IndexKey, IndexStrategy> indexes = ArrayListMultimap.create();

    public void add(IndexStrategy indexStrategy) {
        indexes.put(indexStrategy.index, indexStrategy);
    }
}
