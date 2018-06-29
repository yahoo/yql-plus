/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.inject.AbstractModule;
import com.yahoo.yqlplus.api.trace.RequestTracer;
import com.yahoo.yqlplus.compiler.runtime.ProgramTracer;

public class ProgramTracerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RequestTracer.class).to(ProgramTracer.class);
    }
}
