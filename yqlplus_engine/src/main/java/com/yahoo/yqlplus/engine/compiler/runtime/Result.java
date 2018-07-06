/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import java.util.concurrent.ExecutionException;


public class Result<T> {
    public final YQLError failure;
    public final T result;

    public Result(YQLError failure, T result) {
        this.failure = failure;
        this.result = result;
    }

    public Result(Throwable failure) {
        while ((failure instanceof YQLRuntimeException || failure instanceof ExecutionException) && failure.getCause() != null) {
            failure = failure.getCause();
        }
        this.failure = YQLError.create(failure);
        this.result = null;
    }

    public Result(YQLError failure) {
        this.failure = failure;
        this.result = null;
    }

    public Result(T result) {
        this.failure = null;
        this.result = result;
    }

    public T resolve() {
        if (failure != null) {
            throw new YQLRuntimeException(failure);
        }
        return result;
    }
}
