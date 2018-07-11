/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.api.Namespace;
import com.yahoo.yqlplus.engine.internal.plan.*;
import com.yahoo.yqlplus.engine.source.ExportModuleAdapter;
import com.yahoo.yqlplus.engine.source.SourceAdapter;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;

/**
 * Implement the Namespace binding with a Guice MapBinder.
 */
public class NamespaceAdapter implements SourceNamespace, ModuleNamespace {
    private Namespace namespace;

    private String keyFor(List<String> path) {
        return Joiner.on('.').join(path);
    }

    @Inject
    NamespaceAdapter(Namespace namespace) {
        this.namespace = namespace;
    }

    @Override
    public ModuleType findModule(Location location, ContextPlanner planner, List<String> modulePath) {
        Provider<Exports> moduleProvider = namespace.resolveModule(modulePath);
        if (moduleProvider == null) {
            return null;
        }
        return new ExportModuleAdapter(keyFor(modulePath), moduleProvider.get().getClass(), moduleProvider::get);
    }

    @Override
    public SourceType findSource(Location location, ContextPlanner planner, List<String> sourcePath) {
        Provider<Source> sourceProvider = namespace.resolveSource(sourcePath);
        if (sourceProvider == null) {
            return null;
        }
        return new SourceAdapter(keyFor(sourcePath), sourceProvider.get().getClass(), sourceProvider::get);
//        SourceUnitGenerator adapter = new SourceUnitGenerator(planner.getGambitScope());
//        return adapter.apply(sourcePath, sourceProvider);
    }
}
