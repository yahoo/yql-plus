/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.flow.internal.dynalink;

import org.dynalang.dynalink.DynamicLinker;
import org.dynalang.dynalink.DynamicLinkerFactory;
import org.dynalang.dynalink.MonomorphicCallSite;
import org.dynalang.dynalink.linker.GuardingDynamicLinker;
import org.dynalang.dynalink.support.CallSiteDescriptorFactory;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public class FlowBootstrapper {
    private static final DynamicLinker dynamicLinker;

    static {
        final DynamicLinkerFactory factory = new DynamicLinkerFactory();
        final GuardingDynamicLinker jsonNodeLinker = new JsonNodeLinker();
        factory.setPrioritizedLinkers(jsonNodeLinker);
        dynamicLinker = factory.createLinker();
    }

    public static CallSite publicBootstrap(@SuppressWarnings("unused") MethodHandles.Lookup caller, String name, MethodType type) {
        return dynamicLinker.link(
                new MonomorphicCallSite(CallSiteDescriptorFactory.create(MethodHandles.publicLookup(), name, type)));
    }

    public static CallSite bootstrap(MethodHandles.Lookup caller, String name, MethodType type) {
        return dynamicLinker.link(
                new MonomorphicCallSite(CallSiteDescriptorFactory.create(caller, name, type)));
    }
}
