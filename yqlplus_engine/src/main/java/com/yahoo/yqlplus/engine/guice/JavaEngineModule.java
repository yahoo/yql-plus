/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.inject.AbstractModule;

public class JavaEngineModule extends AbstractModule {
    @Override
    protected void configure() {
        // This is now an assembly
        install(new EngineThreadPoolModule());
        install(new ExecutionScopeModule());
        install(new PlannerCompilerModule());
        install(new ProgramTracerModule());
        install(new SearchNamespaceModule());
        install(new PhysicalOperatorBuiltinsModule());
        install(new SourceApiModule());
    }


}
