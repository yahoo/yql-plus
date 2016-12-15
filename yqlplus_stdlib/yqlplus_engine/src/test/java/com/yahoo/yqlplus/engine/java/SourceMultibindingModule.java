/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.yqlplus.api.Source;

import java.util.Map;

/**
 * Convenience for binding a bunch of names to source classes.
 */
public class SourceMultibindingModule extends AbstractModule {
    private Map<String, Class<? extends Source>> sources;

    public SourceMultibindingModule(Map<String, Class<? extends Source>> sources) {
        this.sources = sources;
    }

    @SuppressWarnings("unchecked")
    public SourceMultibindingModule(Object... kvPairs) {
        ImmutableMap.Builder<String, Class<? extends Source>> sources = ImmutableMap.builder();
        for (int i = 0; i < kvPairs.length; i += 2) {
            sources.put((String) kvPairs[i], (Class<? extends Source>) kvPairs[i + 1]);
        }
        this.sources = sources.build();
    }

    @Override
    protected void configure() {
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        for (Map.Entry<String, Class<? extends Source>> e : sources.entrySet()) {
            sourceBindings.addBinding(e.getKey()).to(e.getValue());
        }
    }
}
