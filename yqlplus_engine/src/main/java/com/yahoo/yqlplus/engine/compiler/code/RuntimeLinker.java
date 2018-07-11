/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.engine.compiler.runtime.FieldWriter;
import org.dynalang.dynalink.CallSiteDescriptor;
import org.dynalang.dynalink.linker.GuardedInvocation;
import org.dynalang.dynalink.linker.LinkRequest;
import org.dynalang.dynalink.linker.LinkerServices;
import org.dynalang.dynalink.support.Guards;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.StringTokenizer;

public class RuntimeLinker {
    private final Class<?> clazz;
    private final MethodHandle property;
    private final MethodHandle propertyString;
    private final MethodHandle propertyStringDefault;
    private final MethodHandle index;
   // private final MethodHandle serializeJson;
   // private final MethodHandle serializeTBin;
    private final MethodHandle mergeIntoFieldWriter;
    private final MethodHandle getFieldNames;

    public RuntimeLinker(Class<?> clazz, Class<? extends RuntimeWidget> widget) {
        this.clazz = clazz;
        try {
            RuntimeWidget widgetInstance = widget.newInstance();
//            public abstract Object property(Object source, String propertyName);
//            public abstract Object index(Object source, Object index);
//            public abstract void serializeJson(Object source, JsonGenerator generator);
//            public abstract void mergeIntoFieldWriter(Object source, FieldWriter writer);
//            public abstract Iterable<String> getFieldNames(Object source);
            final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            property = lookup.findVirtual(widget, "propertyObject", MethodType.methodType(Object.class, Object.class, Object.class))
                    .bindTo(widgetInstance);
            propertyString = lookup.findVirtual(widget, "property", MethodType.methodType(Object.class, Object.class, String.class))
                    .bindTo(widgetInstance);
            propertyStringDefault = lookup.findVirtual(widget, "property", MethodType.methodType(Object.class, Object.class, String.class, Object.class))
                    .bindTo(widgetInstance);
            index = lookup.findVirtual(widget, "index", MethodType.methodType(Object.class, Object.class, Object.class))
                    .bindTo(widgetInstance);

          //  Comment out for now since the code is targeting for gateway implementation
          //  Gateway is not fully implemented and the code will fail some tests of current none-gateway use cases
          //  serializeJson = lookup.findVirtual(widget, "serializeJson", MethodType.methodType(void.class, Object.class, JsonGenerator.class))
          //          .bindTo(widgetInstance);
          //  serializeTBin = lookup.findVirtual(widget, "serializeTBin", MethodType.methodType(void.class, Object.class, TBinEncoder.class))
          //          .bindTo(widgetInstance);
            mergeIntoFieldWriter = lookup.findVirtual(widget, "mergeIntoFieldWriter", MethodType.methodType(void.class, Object.class, FieldWriter.class))
                    .bindTo(widgetInstance);
            getFieldNames = lookup.findVirtual(widget, "getFieldNames", MethodType.methodType(Iterable.class, Object.class))
                    .bindTo(widgetInstance);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException("Exception creating runtime adapter", e);
        }

    }

    public GuardedInvocation link(LinkRequest linkRequest, LinkerServices linkerServices) {
        CallSiteDescriptor desc = linkRequest.getCallSiteDescriptor();
        // TODO: may want to pay attention to linker instability as a hint to generate more generic forms
        if ("yql".equals(desc.getNameToken(0))) {
            if ("mergeFields".equals(desc.getNameToken(1))) {
                return new GuardedInvocation(mergeIntoFieldWriter,
                        Guards.isOfClass(clazz, mergeIntoFieldWriter.type()));
            } else if("getFieldNames".equals(desc.getNameToken(1))) {
              return new GuardedInvocation(getFieldNames,
                      Guards.isOfClass(clazz, getFieldNames.type()));
            }
// Comment out for now since the code is targeting for gateway implementation
// Gateway is not fully implemented and the code will fail some tests of current none-gateway use cases
//            else if ("serialize".equals(desc.getNameToken(1))) {
//                if("json".equals(desc.getNameToken(2))) {
//                    return new GuardedInvocation(serializeJson,
//                            Guards.isOfClass(clazz, serializeJson.type()));
//                } else if("tbin".equals(desc.getNameToken(2))) {
//                    return new GuardedInvocation(serializeJson,
//                            Guards.isOfClass(clazz, serializeTBin.type()));
//                }
//            }
        }
        if (!"dyn".equals(desc.getNameToken(0))) {
            return null;
        }
        // getProp / getElem
        String operation = desc.getNameToken(1);
        if (operation.contains("|")) {
            StringTokenizer tokenizer = new StringTokenizer(operation, "|");
            while (tokenizer.hasMoreElements()) {
                GuardedInvocation result = getGuardedInvocation(desc, tokenizer.nextToken());
                if (result != null) {
                    return result;
                }
            }
            return null;
        } else {
            return getGuardedInvocation(desc, operation);
        }
    }

    private GuardedInvocation getGuardedInvocation(CallSiteDescriptor desc, String operation) {
        MethodType targetType = desc.getMethodType();
        MethodHandle base = this.property;
        if (targetType.parameterCount() > 1 && targetType.parameterArray()[1] == String.class) {
            base = this.propertyString;
        }
        if ("getProp".equals(operation) && desc.getNameTokenCount() > 2) {
            String property = desc.getNameToken(2);
            MethodHandle handle = MethodHandles.insertArguments(base, 1, property);
            return new GuardedInvocation(handle,
                    Guards.isOfClass(clazz, handle.type()));
        } else if("getPropDefault".equals(operation) && desc.getNameTokenCount() > 2) {
            base = this.propertyStringDefault;
            String property = desc.getNameToken(2);
            MethodHandle handle = MethodHandles.insertArguments(base, 1, property);
            return new GuardedInvocation(handle,
                    Guards.isOfClass(clazz, handle.type()));
        } else if("getPropDefault".equals(operation)) {
            String property = desc.getNameToken(2);
            MethodHandle handle = MethodHandles.insertArguments(base, 1, property);
            return new GuardedInvocation(propertyStringDefault,
                    Guards.isOfClass(clazz, handle.type()));
        } else if ("getProp".equals(operation)) {
            MethodHandle target = index.asType(targetType);
            return new GuardedInvocation(target,
                    Guards.isOfClass(clazz, target.type()));
        } else {
            return null;
        }
    }
}
