/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.sample.integration;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.trace.RequestTracer;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.java.Person;

import java.util.List;

public class SimpleSource implements Source {
    // a per-method Tracer injection would be nice, too, but this is just testing ExecuteScoped
    private final RequestTracer tracer;

    @Inject
    SimpleSource(RequestTracer tracer) {
        this.tracer = tracer;
    }

    @Query
    public List<Person> scan() {
        Tracer trace = tracer.start("MINE", "MINE");
        try {
            return ImmutableList.of(new Person("1", "joe", 0));
        } finally {
            trace.fine("Done scanning");
            trace.end();
        }
    }
}
