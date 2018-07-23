/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.guice;


import com.google.inject.Provider;

/**
 * Use with Guice and @ExcecuteScoped to create bindings for things that will be seeded per program execution.
 */
public final class SeededKeyProvider {
    private static final Provider<Object> SEEDED_KEY_PROVIDER =
            new Provider<Object>() {
                public Object get() {
                    throw new IllegalStateException("If you got here then it means that" +
                            " your code asked for scoped object which should have been" +
                            " explicitly seeded.");
                }
            };

    @SuppressWarnings("unchecked")
    public static <T> Provider<T> seededKeyProvider() {
        return (Provider<T>) SEEDED_KEY_PROVIDER;
    }
}
