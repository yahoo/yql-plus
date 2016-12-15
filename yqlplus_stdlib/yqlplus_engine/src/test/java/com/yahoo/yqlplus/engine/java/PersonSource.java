/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PersonSource implements Source {
    
    private static final AtomicInteger index = new AtomicInteger(0);
    private List<Person> items;

    public PersonSource(List<Person> items) {
        this.items = items;
    }

    @Query
    public List<Person> scan() {
        index.getAndIncrement();
        return items;
    }
    
    public static void resetIndex() {
        index.set(0);
    }
    
    public static int getIndex() {
        return index.get();
    }
}
