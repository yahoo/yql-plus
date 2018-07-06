/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Emitter;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.java.Person;

import java.util.List;

public class TracingSource implements Source {
    @Query
    public List<Person> scan(@Emitter Tracer tracer) {
        try (Tracer trace = tracer.start("MINE", "MINE")) {
            trace.fine("Done scanning");
            return ImmutableList.of(new Person("1", "joe", 0));
        }
    }
}

