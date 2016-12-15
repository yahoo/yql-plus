/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.yahoo.yqlplus.engine.internal.plan.ModuleNamespace;
import com.yahoo.yqlplus.engine.internal.plan.ModuleType;
import com.yahoo.yqlplus.engine.internal.plan.SourceNamespace;
import com.yahoo.yqlplus.engine.internal.plan.SourceType;

public class SearchNamespaceModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<String, SourceType> sourceTypeBindings = MapBinder.newMapBinder(binder(), String.class, SourceType.class);
        MapBinder<String, ModuleType> moduleTypeBindings = MapBinder.newMapBinder(binder(), String.class, ModuleType.class);
        MapBinder<String, SourceNamespace> prefixSourceBindings = MapBinder.newMapBinder(binder(), String.class, SourceNamespace.class);
        MapBinder<String, ModuleNamespace> prefixModuleBindings = MapBinder.newMapBinder(binder(), String.class, ModuleNamespace.class);
        Multibinder<SourceNamespace> sourceNamespaceMultibinder = Multibinder.newSetBinder(binder(), SourceNamespace.class);
        Multibinder<ModuleNamespace> moduleNamespaceMultibinder = Multibinder.newSetBinder(binder(), ModuleNamespace.class);
        bind(SourceNamespace.class).to(SearchNamespaceAdapter.class);
        bind(ModuleNamespace.class).to(SearchNamespaceAdapter.class);
    }
}
