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
import com.yahoo.yqlplus.engine.api.DependencyNotFoundException;
import com.yahoo.yqlplus.engine.api.Namespace;
import com.yahoo.yqlplus.engine.internal.plan.*;
import com.yahoo.yqlplus.engine.internal.source.ExportUnitGenerator;
import com.yahoo.yqlplus.engine.internal.source.SourceUnitGenerator;
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
            throw new DependencyNotFoundException(location, "No module '%s' found", keyFor(modulePath));
        }
        ExportUnitGenerator adapter = new ExportUnitGenerator(planner.getGambitScope());
        return adapter.apply(modulePath, moduleProvider);
    }

    @Override
    public SourceType findSource(Location location, ContextPlanner planner, List<String> sourcePath) {
        Provider<Source> sourceProvider = namespace.resolveSource(sourcePath);
        if (sourceProvider == null) {
            throw new DependencyNotFoundException(location, "No source '%s' found", keyFor(sourcePath));
        }
        SourceUnitGenerator adapter = new SourceUnitGenerator(planner.getGambitScope());
        return adapter.apply(sourcePath, sourceProvider);
    }
}
