/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.yahoo.yqlplus.api.annotations.ExecuteScoped;
import com.yahoo.yqlplus.engine.internal.scope.ExecutionScoper;
import com.yahoo.yqlplus.engine.internal.util.ScopingExecutor;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;

import java.util.concurrent.ExecutorService;

public class ExecutionScopeModule extends AbstractModule {
    private static final ExecutionScoper SCOPE = new ExecutionScoper();

    @Override
    protected void configure() {
        bindScope(ExecuteScoped.class, SCOPE);
        bind(ExecutionScoper.class).toInstance(SCOPE);
        bind(ExecutionScope.class).toProvider(SCOPE.getExecutionScope()).in(ExecuteScoped.class);
    }

    @Named("scopedWork")
    @Provides
    ListeningExecutorService provideWorkService(@Named("work") ExecutorService service, final ExecutionScoper scoper) {
        return MoreExecutors.listeningDecorator(new ScopingExecutor(service) {
            @Override
            protected Runnable scope(Runnable task) {
                return scoper.continueScope(task);
            }
        });
    }
}
