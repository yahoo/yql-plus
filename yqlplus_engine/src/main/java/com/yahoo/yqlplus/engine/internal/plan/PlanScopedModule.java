/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.annotations.ExecuteScoped;
import com.yahoo.yqlplus.api.guice.SeededKeyProvider;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.TaskContext;

public class PlanScopedModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TaskContext.class).annotatedWith(Names.named("rootContext"))
                .toProvider(SeededKeyProvider.seededKeyProvider())
                .in(ExecuteScoped.class);
        bind(ListeningExecutorService.class).annotatedWith(Names.named("programExecutor"))
                .toProvider(SeededKeyProvider.seededKeyProvider())
                .in(ExecuteScoped.class);
        bind(ListeningExecutorService.class).annotatedWith(Names.named("programTimeout"))
                .toProvider(SeededKeyProvider.seededKeyProvider())
                .in(ExecuteScoped.class);
        bind(TaskMetricEmitter.class).annotatedWith(Names.named("task"))
                .toProvider(SeededKeyProvider.seededKeyProvider())
                .in(ExecuteScoped.class);
        bind(Tracer.class)
                .toProvider(SeededKeyProvider.seededKeyProvider())
                .in(ExecuteScoped.class);
        bind(String.class).annotatedWith(Names.named("programName"))
                .toProvider(SeededKeyProvider.seededKeyProvider())
                .in(ExecuteScoped.class);
    }
}
