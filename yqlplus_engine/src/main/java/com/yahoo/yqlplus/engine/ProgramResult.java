/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.yahoo.yqlplus.api.trace.Tracer;

import java.util.concurrent.CompletableFuture;

public interface ProgramResult {
    Iterable<String> getResultNames();

    CompletableFuture<YQLResultSet> getResult(String name);

    CompletableFuture<Tracer> getEnd();

}
