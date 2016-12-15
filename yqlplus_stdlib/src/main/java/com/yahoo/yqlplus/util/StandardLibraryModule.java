/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.util;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.yqlplus.api.Exports;

public class StandardLibraryModule extends AbstractModule {

    @Override
    protected void configure() {
        MapBinder<String, Exports> exportsBindings = MapBinder.newMapBinder(binder(), String.class, Exports.class);
        exportsBindings.addBinding("datetime").to(DateTime.class);
    }
}