/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.sample.integration;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.yqlplus.api.Source;

/**
 * To change this template use File | Settings | File Templates.
 */
public class SourceModule1 extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        sourceBindings.addBinding("simple").to(SimpleSource.class);
    }
}
