/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Trace;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.java.Person;

public class MethodTracingSource implements Source {
    @Query
    public List<Person> scan(@Trace("MINE") Tracer trace) throws InterruptedException {
        Thread.sleep(500);
        trace.fine("Done scanning");
        trace.end();
        return ImmutableList.of(new Person("1", "joe", 0));
    }
}