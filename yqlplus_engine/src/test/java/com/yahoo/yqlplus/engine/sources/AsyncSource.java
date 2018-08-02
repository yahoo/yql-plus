/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.TimeoutMilliseconds;
import com.yahoo.yqlplus.engine.java.Person;

import java.util.List;

public class AsyncSource implements Source {
    @Query
    public ListenableFuture<List<Person>> scan(@TimeoutMilliseconds long timeoutMs) {
        // checking minimum is dodgy and leads to failures
        return Futures.immediateFuture(ImmutableList.of(new Person("1", "joe", 1)));
    }

    @Query
    public ListenableFuture<Person> lookup(@Key("id") String id) {
        if ("1".equals(id)) {
            return Futures.immediateFuture(new Person("1", "joe", 1));
        } else if ("3".equals(id)) {
            return Futures.immediateFuture(new Person("3", "smith", 1));
        } else {
            return Futures.immediateFuture(null);
        }
    }
}
