/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;

import java.util.Arrays;
import java.util.List;

public final class YQLUnionType extends YQLType {
    private final List<YQLType> choices;

    private static String createName(List<YQLType> types) {
        StringBuilder out = new StringBuilder("union<");
        boolean first = true;
        for (YQLType type : types) {
            if (!first) {
                out.append(",");
            }
            first = false;
            out.append(type.getName());
        }
        out.append(">");
        return out.toString();

    }

    YQLUnionType(Annotations annotations, List<YQLType> types) {
        super(annotations, YQLCoreType.UNION, createName(types));
        this.choices = ImmutableList.copyOf(types);
    }

    public List<YQLType> getChoices() {
        return choices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YQLUnionType that = (YQLUnionType) o;

        return choices.equals(that.choices);
    }

    @Override
    public int hashCode() {
        return choices.hashCode();
    }

    @Override
    public void hashTo(Hasher digest) {
        super.hashTo(digest);
        for (YQLType type : choices) {
            type.hashTo(digest);
        }
    }

    public static YQLType create(YQLType... types) {
        return create(Arrays.asList(types));
    }

    public static YQLType create(Iterable<YQLType> types) {
        List<YQLType> inputs = Lists.newArrayList();
        boolean optional = false;
        for (YQLType input : types) {
            optional = optional || YQLOptionalType.is(input);
            input = YQLOptionalType.deoptional(input);
            if (!inputs.contains(input)) {
                inputs.add(input);
            }
        }
        YQLType output;
        if (inputs.size() == 1) {
            output = inputs.get(0);
        } else {
            output = new YQLUnionType(Annotations.EMPTY, inputs);
        }
        return optional ? YQLOptionalType.create(output) : output;

    }

    public static boolean is(YQLType type) {
        return type instanceof YQLUnionType;
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLUnionType(newAnnotations, choices);
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        if (source.getCoreType() == getCoreType()) {
            YQLUnionType other = (YQLUnionType) source;
            for (YQLType choice : other.choices) {
                if (!internalAssignableFrom(choice)) {
                    return false;
                }
            }
            return true;
        } else {
            for (YQLType choice : choices) {
                if (choice.isAssignableFrom(source)) {
                    return true;
                }
            }
            return false;
        }
    }
}
