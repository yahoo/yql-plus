/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.TimeoutMilliseconds;
import com.yahoo.yqlplus.engine.java.Person;

import java.util.List;

public class InnerSource implements Source {
    @Query
    public List<Person> scan(@TimeoutMilliseconds long timeoutMs) {
        return ImmutableList.of(new Person("1", "joe", 1));
    }

    @Query
    public Person lookup(@Key("id") String id) {
        if ("1".equals(id)) {
            return new Person("1", "joe", 1);
        } else if ("3".equals(id)) {
            return new Person("3", "smith", 1);
        } else {
            return null;
        }
    }

    @Query
    public Person lookup(@Key("iid") Integer id) {
        return lookup(String.valueOf(id));
    }
}
