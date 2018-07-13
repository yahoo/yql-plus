/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.compiler.code.TypeAdaptingWidget;

import java.util.List;

/**
 * Simplified entry point for non-Guice users to create a compiler.
 */
public final class YQLPlusEngine {
    private YQLPlusEngine() {
    }

    public static class Builder {
        final BindingNamespace namespace = new BindingNamespace();
        final List<TypeAdaptingWidget> adapters = Lists.newArrayList();

        public Builder bind(Object... kvPairs) {
            namespace.bind(kvPairs);
            return this;
        }

        public Builder addAdapter(TypeAdaptingWidget adapter) {
            this.adapters.add(adapter);
            return this;
        }

        public Builder addAdapters(TypeAdaptingWidget... adapters) {
            for(TypeAdaptingWidget adapter : adapters) {
                addAdapter(adapter);
            }
            return this;
        }

        public BindingNamespace binder() {
            return namespace;
        }

        public YQLPlusCompiler build() {
            return new YQLPlusCompiler(adapters, namespace, namespace, namespace);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static YQLPlusCompiler createCompiler(ModuleNamespace modules, SourceNamespace sources) {
        return new YQLPlusCompiler(ImmutableList.of(), sources, modules, name -> null);
    }
}
