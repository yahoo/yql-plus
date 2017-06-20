/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.example.apis;

import java.util.List;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.cloud.metrics.api.DummyStandardRequestEmitter;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.RequestEvent;
import com.yahoo.cloud.metrics.api.RequestMetricSink;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.guice.JavaEngineModule;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

public class SourceModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new JavaEngineModule());
        bind(ViewRegistry.class).toInstance(new ViewRegistry() {
            @Override
            public OperatorNode<SequenceOperator> getView(List<String> name) {
                return null;
            }
        });
        bind(TaskMetricEmitter.class).toInstance(
                new DummyStandardRequestEmitter(new MetricDimension(), new RequestMetricSink() {
                    @Override
                    public void emitRequest(final RequestEvent arg0) {
                    }            
                  }).start("program", "program"));
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        sourceBindings.addBinding("weather").toInstance(new WeatherSource());
        sourceBindings.addBinding("geo").toInstance(new GeoSource());
    }
}
