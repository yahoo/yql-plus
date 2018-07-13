/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.engine.api.Namespace;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.compiler.code.ASMClassSourceModule;
import com.yahoo.yqlplus.engine.guice.NamespaceAdapter;
import com.yahoo.yqlplus.engine.internal.plan.ModuleNamespace;
import com.yahoo.yqlplus.engine.internal.plan.SourceNamespace;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Simplified entry point for non-Guice users to create a compiler.
 */
public final class YQLPlusEngine {
    private YQLPlusEngine() {
    }

    private static class BaseModule extends AbstractModule {
        @Override
        protected void configure() {

            install(new ASMClassSourceModule());
            bind(ViewRegistry.class).toInstance(new ViewRegistry() {
                @Override
                public OperatorNode<SequenceOperator> getView(List<String> name) {
                    return null;
                }
            });
            bind(SourceNamespace.class).to(NamespaceAdapter.class);
            bind(ModuleNamespace.class).to(NamespaceAdapter.class);

        }
    }


    private static final class NamespaceModule extends AbstractModule {
        private final Namespace namespace;

        private NamespaceModule(Namespace namespace) {
            this.namespace = namespace;
        }

        @Override
        protected void configure() {
            bind(Namespace.class).toInstance(namespace);
        }
    }

    public static YQLPlusCompiler createCompiler(Namespace dependencies) {
        Injector injector = Guice.createInjector(new BaseModule(), new NamespaceModule(dependencies));
        return injector.getInstance(YQLPlusCompiler.class);
    }
}
