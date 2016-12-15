/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * All this will be replaced by TDL.
 */
public final class YQLSchema {
    private static final Map<String, YQLType> baseTypes = createBaseTypes();

    private static final Map<String, YQLType> createBaseTypes() {
        Map<String, YQLType> base = new LinkedHashMap<>();
        for (Field type : YQLBaseType.class.getFields()) {
            if (Modifier.isPublic(type.getModifiers()) && Modifier.isStatic(type.getModifiers()) && YQLType.class.isAssignableFrom(type.getType())) {
                try {
                    YQLType value = (YQLType) type.get(null);
                    base.put(value.getName(), value);
                } catch (IllegalAccessException e) {
                    // this should never happen
                }
            }
        }
        return Collections.unmodifiableMap(base);
    }

    private final Map<String, YQLType> types = new HashMap<>();

    public YQLType get(String name) {
        if (baseTypes.containsKey(name)) {
            return baseTypes.get(name);
        }
        return types.get(name);
    }

    public void declare(YQLType type) {
        if (get(type.getName()) != null) {
            throw new YQLTypeException("Cannot override already declared type '" + type.getName() + "'");
        }
        types.put(type.getName(), type);
    }

    public <T extends YQLType> T declareIfMissing(T type) {
        YQLType result = get(type.getName());
        if (result == null) {
            declare(type);
            return type;
        } else if (!result.equals(type)) {
            throw new YQLTypeException("Declare-if-missing has conflicting type: " + result + " != " + type);
        }
        return (T) result;
    }


    public YQLMapType map(YQLType key, YQLType value) {
        return declareIfMissing(YQLMapType.create(key, value));

    }

    public YQLArrayType array(YQLType element) {
        return declareIfMissing(new YQLArrayType(element));
    }

    public YQLType union(YQLType... types) {
        // TODO: require that a union be itself non-optional (any optional types inside the union should be pushed up)
        //   union<int,optional<string>> -> optional<union<int,string>>
        // until that is done, do not permit unions to be used
        return YQLUnionType.create(types);
    }

    public YQLStructType.Builder struct() {
        return YQLStructType.builder();
    }

    public YQLType addStruct(YQLStructType type) {
        return declareIfMissing(type);
    }
}
