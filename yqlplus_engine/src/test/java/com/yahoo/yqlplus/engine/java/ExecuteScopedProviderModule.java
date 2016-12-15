/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.yqlplus.api.annotations.ExecuteScoped;

public class ExecuteScopedProviderModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    @ExecuteScoped
    public Foo makeFoo() {
        return new Foo();
    }

    public static class Foo {
        public String str;
    }
}
