/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.api;

import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;

import com.google.inject.Provider;
import java.util.List;

/**
 * The API for telling the engine about views, sources, transformation functions, and scalar functions.
 */
public interface Namespace extends ViewRegistry {
    /**
     * The engine will call resolveSource to locate sources.
     * <p/>
     * It will then use reflection to discover @Query method to identify ways to query the source.
     * <p/>
     * It will invoke the Provider once at compile time to get an instance to reflect upon, and again once per invocation
     * of the source.
     *
     * @param path source path
     * @return A provider of source instances
     * @see com.yahoo.yqlplus.api.Source
     * @see com.yahoo.yqlplus.api.Exports
     * @see com.yahoo.yqlplus.api.annotations.Query
     * @see com.yahoo.yqlplus.api.annotations.Export
     * @see com.yahoo.yqlplus.api.annotations.Key
     */
    Provider<Source> resolveSource(List<String> path);

    /**
     * The engine will call resolveModule to locate objects (and their classes) which export functions.
     * <p/>
     * A class must implement Exports to be eligible for querying for @Exports methods.
     * <p/>
     * Functions are used for both transforms (pipes) and UDFs in expressions.
     *
     * @param path
     * @return
     * @see com.yahoo.yqlplus.api.Source
     * @see com.yahoo.yqlplus.api.Exports
     * @see com.yahoo.yqlplus.api.annotations.Query
     * @see com.yahoo.yqlplus.api.annotations.Export
     * @see com.yahoo.yqlplus.api.annotations.Key
     */
    Provider<Exports> resolveModule(List<String> path);
}
