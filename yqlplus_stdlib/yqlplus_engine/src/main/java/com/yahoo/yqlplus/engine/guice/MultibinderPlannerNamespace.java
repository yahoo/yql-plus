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
import com.yahoo.yqlplus.engine.internal.plan.*;
import com.yahoo.yqlplus.engine.internal.source.ExportUnitGenerator;
import com.yahoo.yqlplus.engine.internal.source.SourceUnitGenerator;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.List;
import java.util.Map;

/**
 * Implement the Namespace binding with a Guice MapBinder.
 */
public class MultibinderPlannerNamespace implements SourceNamespace, ModuleNamespace {
    private final Map<String, Provider<Source>> sourceBindings;
    private final Map<String, Provider<Exports>> exportsBindings;

    private String keyFor(List<String> path) {
        return Joiner.on('.').join(path);
    }

    @Inject
    MultibinderPlannerNamespace(Map<String, Provider<Exports>> exportsBindings, Map<String, Provider<Source>> sourceBindings) {
        this.exportsBindings = exportsBindings;
        this.sourceBindings = sourceBindings;
    }

    @Override
    public ModuleType findModule(Location location, ContextPlanner planner, List<String> modulePath) {
        Provider<Exports> moduleProvider = exportsBindings.get(keyFor(modulePath));
        if (moduleProvider == null) {
            throw new ProgramCompileException(location, "No source '%s' found", keyFor(modulePath));
        }
        ExportUnitGenerator adapter = new ExportUnitGenerator(planner.getGambitScope());
        return adapter.apply(modulePath, moduleProvider);
    }

    @Override
    public SourceType findSource(Location location, ContextPlanner planner, List<String> sourcePath) {
        Provider<Source> sourceProvider = sourceBindings.get(keyFor(sourcePath));
        if (sourceProvider == null) {
            throw new DependencyNotFoundException(location, "No source '%s' found", keyFor(sourcePath));
        }
        SourceUnitGenerator adapter = new SourceUnitGenerator(planner.getGambitScope());
        return adapter.apply(sourcePath, sourceProvider);
    }
}
