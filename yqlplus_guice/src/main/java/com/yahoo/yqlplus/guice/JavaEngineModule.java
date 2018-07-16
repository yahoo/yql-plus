/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.guice;

import com.google.inject.AbstractModule;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public class JavaEngineModule extends AbstractModule {
    @Override
    protected void configure() {
        // This is now an assembly
        install(new ASMClassSourceModule());
        install(new SearchNamespaceModule());
        install(new SourceApiModule());
        bind(ViewRegistry.class).toInstance(new ViewRegistry() {
            @Override
            public OperatorNode<SequenceOperator> getView(List<String> name) {
                return null;
            }
        });
    }


}
