/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.scope;

import com.google.inject.Key;

/**
 * Use an ExecutionScope to provide @ExecuteScoped items to the engine -- items which are unique per program execution.
 *
 * @see com.yahoo.yqlplus.api.annotations.ExecuteScoped
 * @see com.yahoo.yqlplus.engine.CompiledProgram#run(java.util.Map, boolean, ExecutionScope)
 */
public interface ExecutionScope {
    <T> T get(final Key<T> key);
}