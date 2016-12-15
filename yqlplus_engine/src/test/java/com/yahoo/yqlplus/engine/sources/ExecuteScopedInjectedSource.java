/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.inject.Inject;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.ExecuteScopedProviderModule;

public class ExecuteScopedInjectedSource implements Source {

    @Inject
    private ExecuteScopedProviderModule.Foo foo;

    @Query
    public ExecuteScopedProviderModule.Foo scan() {
        return foo;
    }
}
