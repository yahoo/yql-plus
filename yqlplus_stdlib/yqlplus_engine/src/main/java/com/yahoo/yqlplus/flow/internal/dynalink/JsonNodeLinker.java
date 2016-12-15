/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.flow.internal.dynalink;

import com.fasterxml.jackson.databind.JsonNode;
import org.dynalang.dynalink.CallSiteDescriptor;
import org.dynalang.dynalink.linker.GuardedInvocation;
import org.dynalang.dynalink.linker.LinkRequest;
import org.dynalang.dynalink.linker.LinkerServices;
import org.dynalang.dynalink.linker.TypeBasedGuardingDynamicLinker;
import org.dynalang.dynalink.support.Guards;
import org.dynalang.dynalink.support.Lookup;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;

public class JsonNodeLinker implements TypeBasedGuardingDynamicLinker {
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

    private static MethodHandle READ_PROPERTY = Lookup.findOwnStatic(MethodHandles.lookup(), "readProperty", Object.class, JsonNode.class, String.class);

    public boolean canLinkType(Class<?> type) {
        return JsonNode.class.isAssignableFrom(type);
    }

    @Override
    public GuardedInvocation getGuardedInvocation(LinkRequest linkRequest, LinkerServices linkerServices) throws Exception {
        if (!(linkRequest.getReceiver() instanceof JsonNode)) {
            return null;
        }
        JsonNode node = (JsonNode) linkRequest.getReceiver();
        CallSiteDescriptor desc = linkRequest.getCallSiteDescriptor();
        if ("dyn".equals(desc.getNameToken(0)) && "getProp".equals(desc.getNameToken(1)) && desc.getNameTokenCount() > 2) {
            String property = desc.getNameToken(2);
            MethodHandle handle = MethodHandles.insertArguments(READ_PROPERTY, 1, property);
            return new GuardedInvocation(handle, Guards.isInstance(JsonNode.class, handle.type()));
        }
        return null;
    }
}
