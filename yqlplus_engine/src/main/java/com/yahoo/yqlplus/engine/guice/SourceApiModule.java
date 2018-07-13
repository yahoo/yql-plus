/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.ModuleNamespace;
import com.yahoo.yqlplus.engine.SourceNamespace;

/**
 * Implement the Namespace binding with a Guice MapBinder.
 */
public class SourceApiModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        MapBinder<String, Exports> exportsBindings = MapBinder.newMapBinder(binder(), String.class, Exports.class);
        Multibinder<SourceNamespace> sourceNamespaceMultibinder = Multibinder.newSetBinder(binder(), SourceNamespace.class);
        Multibinder<ModuleNamespace> moduleNamespaceMultibinder = Multibinder.newSetBinder(binder(), ModuleNamespace.class);
        sourceNamespaceMultibinder.addBinding().to(MultibinderPlannerNamespace.class);
        moduleNamespaceMultibinder.addBinding().to(MultibinderPlannerNamespace.class);
    }
}