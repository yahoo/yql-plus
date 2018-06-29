/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.Callable;

public interface GambitRuntime {
    ListenableFuture<List<Object>> scatter(List<Callable<Object>> targets);

    ListenableFuture<List<Object>> scatterAsync(List<Callable<ListenableFuture<Object>>> targets);

    ListenableFuture<Object> fork(Callable<Object> target);

    ListenableFuture<Object> forkAsync(Callable<ListenableFuture<Object>> target);
}
