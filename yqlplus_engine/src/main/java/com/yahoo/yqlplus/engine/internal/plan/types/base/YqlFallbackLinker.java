/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import org.dynalang.dynalink.CallSiteDescriptor;
import org.dynalang.dynalink.linker.GuardedInvocation;
import org.dynalang.dynalink.linker.GuardingDynamicLinker;
import org.dynalang.dynalink.linker.LinkRequest;
import org.dynalang.dynalink.linker.LinkerServices;
import org.dynalang.dynalink.support.Guards;
import org.dynalang.dynalink.support.Lookup;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;

public class YqlFallbackLinker implements GuardingDynamicLinker {
    public static Object readProperty(JsonNode node, String property) throws IOException {
        JsonNode result = node.get(property);
        if (result == null) {
            return null;
        }
        switch (result.getNodeType()) {
            case NUMBER:
                return result.numberValue();
            case BOOLEAN:
                return result.booleanValue();
            case NULL:
                return null;
            case STRING:
                return result.asText();
            case BINARY:
                return ByteBuffer.wrap(result.binaryValue());
            case MISSING:
            case OBJECT:
            case POJO:
            case ARRAY:
            default:
                return result;
        }

    }

    public static void mergeFields(Object source, FieldWriter target) {
        throw new UnsupportedOperationException();
    }

    public static void serializeJson(Object source, JsonGenerator target) throws IOException {
        target.writeObject(source);
    }

    private static MethodHandle MERGE_FIELDS = Lookup.findOwnStatic(MethodHandles.lookup(), "mergeFields", void.class, Object.class, FieldWriter.class);
    private static MethodHandle SERIALIZE_JSON = Lookup.findOwnStatic(MethodHandles.lookup(), "serializeJson", void.class, Object.class, JsonGenerator.class);

    @Override
    public GuardedInvocation getGuardedInvocation(LinkRequest linkRequest, LinkerServices linkerServices) throws Exception {
        CallSiteDescriptor desc = linkRequest.getCallSiteDescriptor();
        Class<?> clazz = linkRequest.getReceiver().getClass();
        if ("yql".equals(desc.getNameToken(0))) {
            if ("mergeFields".equals(desc.getNameToken(1))) {
                return new GuardedInvocation(MERGE_FIELDS,
                        Guards.isInstance(clazz, 0, MERGE_FIELDS.type()));
            } else if ("serializeJson".equals(desc.getNameToken(1))) {
                return new GuardedInvocation(SERIALIZE_JSON,
                        Guards.isInstance(clazz, 0, SERIALIZE_JSON.type()));
            }
        }
        return null;
    }
}
