/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.flow.internal.dynalink.FlowBootstrapper;
import com.yahoo.yqlplus.flow.internal.dynalink.JsonNodeLinker;
import org.dynalang.dynalink.DynamicLinker;
import org.dynalang.dynalink.DynamicLinkerFactory;
import org.dynalang.dynalink.MonomorphicCallSite;
import org.dynalang.dynalink.linker.GuardingDynamicLinker;
import org.dynalang.dynalink.support.CallSiteDescriptorFactory;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;

public class Dynamic {

    public static String DYNAMIC_INTERNAL_NAME = "com/yahoo/yqlplus/engine/internal/dynamic/Bootstrap";
    public static String DYNAMIC_BOOTSTRAP_METHOD = "bootstrap";

    public static final Handle H_DYNALIB_BOOTSTRAP =
            new Handle( // For dynamic linking it should delegate to...
                    H_INVOKESTATIC, // ... a static method...
                    Type.getInternalName(FlowBootstrapper.class),
                    "bootstrap",
                    MethodType.methodType(
                            CallSite.class, // ...that will return a CallSite object, ...
                            MethodHandles.Lookup.class, // ... when given a lookup object, ...
                            String.class, // ... the operation name, ...
                            MethodType.class // ... and the signature at the call site.
                    ).toMethodDescriptorString(), false);

    public static final Handle H_BOOTSTRAP =
            new Handle( // For dynamic linking it should delegate to...
                    H_INVOKESTATIC, // ... a static method...
                    DYNAMIC_INTERNAL_NAME,
                    DYNAMIC_BOOTSTRAP_METHOD,
                    MethodType.methodType(
                            CallSite.class, // ...that will return a CallSite object, ...
                            MethodHandles.Lookup.class, // ... when given a lookup object, ...
                            String.class, // ... the operation name, ...
                            MethodType.class // ... and the signature at the call site.
                    ).toMethodDescriptorString(), false);

    public static DynamicLinker createDynamicLinker(ASMClassSource parentLoader) {
        final DynamicLinkerFactory factory = new DynamicLinkerFactory();
        final GuardingDynamicLinker jsonNodeLinker = new JsonNodeLinker();
        factory.setPrioritizedLinkers(new ASMClassSourceLinker(parentLoader), jsonNodeLinker);
        return factory.createLinker();
    }

    public static CallSite link(MethodHandles.Lookup lookup, String name, MethodType type, DynamicLinker linker) {
        return linker.link(new MonomorphicCallSite(CallSiteDescriptorFactory.create(lookup, name, type)));
    }

}
