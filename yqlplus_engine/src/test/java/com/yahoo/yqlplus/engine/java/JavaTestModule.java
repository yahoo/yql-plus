/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.guice.JavaEngineModule;
import com.yahoo.yqlplus.engine.sources.AsyncSource;
import com.yahoo.yqlplus.engine.sources.InnerSource;
import com.yahoo.yqlplus.engine.sources.MapArgumentSource;
import com.yahoo.yqlplus.engine.sources.MapSource;
import com.yahoo.yqlplus.engine.sources.PersonListMakeSource;
import com.yahoo.yqlplus.engine.sources.TracingSource;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public class JavaTestModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new JavaEngineModule());
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        sourceBindings.addBinding("minions").toInstance(new MinionSource(ImmutableList.of(new Minion("1", "2"), new Minion("1", "3"))));
        sourceBindings.addBinding("people").toInstance(new PersonSource(ImmutableList.of(new Person("1", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2))));
        sourceBindings.addBinding("citizen").toInstance(new CitizenSource(ImmutableList.of(new Citizen("1", "German"), new Citizen("2", "Italian"), new Citizen("3", "U.S. American"))));
        sourceBindings.addBinding("peopleWithNullId").toInstance(new PersonSource(ImmutableList.of(new Person(null, "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2))));
        sourceBindings.addBinding("peopleWithEmptyId").toInstance(new PersonSource(ImmutableList.of(new Person("", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2))));
        sourceBindings.addBinding("moreMinions").toInstance(new MinionSource(ImmutableList.of(new Minion("1", "2"), new Minion("1", "3"), new Minion("2", "1"))));
        sourceBindings.addBinding("minionsWithSkipNullSetToFalse").toInstance(new MinionSourceWithSkipNullSetToFalse(ImmutableList.of(new Minion(null, "2"), new Minion("1", "3"), new Minion("2", "1"))));
        sourceBindings.addBinding("noMatchMinions").toInstance(new MinionSource(ImmutableList.of(new Minion("4", "2"), new Minion("4", "3"), new Minion("5", "1"))));
        sourceBindings.addBinding("images").toInstance(new ImageSource(ImmutableList.of(new Image("1", "1.jpg"), new Image("3", "3.jpg"))));
        sourceBindings.addBinding("innersource").to(InnerSource.class);
        sourceBindings.addBinding("asyncsource").to(AsyncSource.class);
        sourceBindings.addBinding("trace").to(TracingSource.class);
        sourceBindings.addBinding("mapsource").to(MapSource.class);
        sourceBindings.addBinding("mapArgumentSource").to(MapArgumentSource.class);
        sourceBindings.addBinding("personList").toInstance(new PersonListMakeSource());
        bind(ViewRegistry.class).toInstance(new ViewRegistry() {
            @Override
            public OperatorNode<SequenceOperator> getView(List<String> name) {
                return null;
            }
        });
    }
}
