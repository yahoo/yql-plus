/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.Maps;
import org.dynalang.dynalink.CallSiteDescriptor;
import org.dynalang.dynalink.linker.GuardedInvocation;
import org.dynalang.dynalink.linker.GuardingDynamicLinker;
import org.dynalang.dynalink.linker.LinkRequest;
import org.dynalang.dynalink.linker.LinkerServices;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

public class ASMClassSourceLinker implements GuardingDynamicLinker {
    private final ASMClassSource classSource;

    private final Map<Class<?>, RuntimeLinker> runtimeLinkers = Maps.newIdentityHashMap();

    public ASMClassSourceLinker(ASMClassSource source) {
        this.classSource = source;
    }


    @Override
    public GuardedInvocation getGuardedInvocation(LinkRequest linkRequest, LinkerServices linkerServices) {
        if (linkRequest.getReceiver() == null) {
            return null;
        }
        CallSiteDescriptor desc = linkRequest.getCallSiteDescriptor();
        if (desc.getNameTokenCount() < 2) {
            return null;
        }
        if ("dyn".equals(desc.getNameToken(0)) && "callMethod".equals(desc.getNameToken(1))) {
            return null;
        }
        Object target = linkRequest.getReceiver();
        Class<?> clazz = target.getClass();
        RuntimeLinker linker = null;
        synchronized (runtimeLinkers) {
            if (runtimeLinkers.containsKey(clazz)) {
                linker = runtimeLinkers.get(clazz);
            } else {
                linker = generateRuntimeLinker(clazz);
                runtimeLinkers.put(clazz, linker);
            }
        }
        return (linker != null) ? linker.link(linkRequest, linkerServices) : null;
    }

    private TypeWidget getTypeWidget(Class<?> clazz) {
        try {
            Field fld = clazz.getField("$TYPE$");
            if (Modifier.isStatic(fld.getModifiers()) && TypeWidget.class.isAssignableFrom(fld.getType())) {
                return (TypeWidget) clazz.getField("$TYPE$").get(null);
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {

        }
        return null;
    }

    private RuntimeLinker generateRuntimeLinker(Class<?> clazz) {
        ASMClassSource childEnvironment = classSource.createChildSource(clazz.getClassLoader());
        EngineValueTypeAdapter typeAdapter = childEnvironment.getValueTypeAdapter();
        TypeWidget widget = getTypeWidget(clazz);
        if (widget == null) {
            widget = typeAdapter.adaptInternal(clazz);
        }
        RuntimeAdapter adapter = new StandardRuntimeAdapter(widget);
        RuntimeWidgetGenerator generator = new RuntimeWidgetGenerator(clazz.getName().replace(".", "_") + "_adapter", childEnvironment);
        generator.adapt(widget, adapter);
        try {
            childEnvironment.build();
            Class<? extends RuntimeWidget> adapterWidget = (Class<? extends RuntimeWidget>) childEnvironment.getGeneratedClass(generator);
            return new RuntimeLinker(clazz, adapterWidget);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch(NegativeArraySizeException e) {
            childEnvironment.dump(System.err);
            throw e;
        }
    }
}
