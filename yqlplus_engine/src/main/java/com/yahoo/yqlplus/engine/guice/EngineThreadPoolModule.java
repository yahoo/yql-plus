/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class EngineThreadPoolModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ExecutorService.class).annotatedWith(Names.named("work")).toInstance(Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("work-%d")
                .build()));

        bind(ScheduledExecutorService.class).annotatedWith(Names.named("timeout")).toInstance(Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("timeout-%d")
                .build()));
    }
}
