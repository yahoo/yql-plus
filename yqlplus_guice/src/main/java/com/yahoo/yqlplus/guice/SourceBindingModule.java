/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;

import java.util.Map;

/**
 * Convenience for binding a bunch of names to source classes.
 */
public class SourceBindingModule extends AbstractModule {
    private Map<String, Source> sourceInstances;
    private Map<String, Exports> exportInstances;
    private Map<String, Class<? extends Source>> sources;
    private Map<String, Class<? extends Exports>> exports;

    @SuppressWarnings("unchecked")
    public SourceBindingModule(Object... kvPairs) {
        ImmutableMap.Builder<String, Source> sourceInstances = ImmutableMap.builder();
        ImmutableMap.Builder<String, Exports> exportsInstances = ImmutableMap.builder();
        ImmutableMap.Builder<String, Class<? extends Source>> sources = ImmutableMap.builder();
        ImmutableMap.Builder<String, Class<? extends Exports>> exports = ImmutableMap.builder();
        for (int i = 0; i < kvPairs.length; i += 2) {
            String key = (String) kvPairs[i];
            if (kvPairs[i + 1] instanceof Source) {
                sourceInstances.put(key, (Source) kvPairs[i + 1]);
                continue;
            } else if (kvPairs[i + 1] instanceof Exports) {
                exportsInstances.put(key, (Exports) kvPairs[i + 1]);
                continue;
            }
            Class<?> clazz = (Class<?>) kvPairs[i + 1];
            if (Source.class.isAssignableFrom(clazz)) {
                sources.put(key, (Class<? extends Source>) kvPairs[i + 1]);
            } else if (Exports.class.isAssignableFrom(clazz)) {
                exports.put(key, (Class<? extends Exports>) kvPairs[i + 1]);
            } else {
                throw new IllegalArgumentException("Don't know how to bind " + clazz);
            }
        }
        this.sources = sources.build();
        this.exports = exports.build();
        this.sourceInstances = sourceInstances.build();
        this.exportInstances = exportsInstances.build();
    }

    @Override
    protected void configure() {
      
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        for (Map.Entry<String, Class<? extends Source>> e : sources.entrySet()) {
            sourceBindings.addBinding(e.getKey()).to(e.getValue());
        }
        for (Map.Entry<String, Source> e : sourceInstances.entrySet()) {
            sourceBindings.addBinding(e.getKey()).toInstance(e.getValue());
        }
        MapBinder<String, Exports> exportsBindings = MapBinder.newMapBinder(binder(), String.class, Exports.class);
        for (Map.Entry<String, Class<? extends Exports>> e : exports.entrySet()) {
            exportsBindings.addBinding(e.getKey()).to(e.getValue());
        }
        for (Map.Entry<String, Exports> e : exportInstances.entrySet()) {
            exportsBindings.addBinding(e.getKey()).toInstance(e.getValue());
        }
    }
}
