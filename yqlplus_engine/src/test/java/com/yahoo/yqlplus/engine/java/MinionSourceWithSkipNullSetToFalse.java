/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import java.util.List;

public class MinionSourceWithSkipNullSetToFalse implements Source {

    private final List<Minion> items;
    private final Multimap<String, Minion> minionMap = ArrayListMultimap.create();

    MinionSourceWithSkipNullSetToFalse(List<Minion> items) {
        this.items = items;
        for (Minion item : items) {
            minionMap.put(item.master_id, item);
        }
    }

    @Query
    public List<Minion> scan() {
        return items;
    }

    @Query
    public Iterable<Minion> get(@Key(value = "master_id", skipNull = false) String master_id) {
        return minionMap.get(master_id);
    }
}
