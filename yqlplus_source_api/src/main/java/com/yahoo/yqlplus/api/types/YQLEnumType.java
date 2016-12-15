/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;

import java.util.List;

public final class YQLEnumType extends YQLType {
    private final List<String> symbols;

    public YQLEnumType(Annotations annnotations, List<String> symbols) {
        super(annnotations, YQLCoreType.ENUM, "enum<" + join(symbols) + ">");
        this.symbols = symbols;
    }

    public YQLEnumType(List<String> symbols) {
        this(Annotations.EMPTY, symbols);
    }

    private YQLEnumType(Annotations annotations, String name, List<String> symbols) {
        super(annotations, YQLCoreType.ENUM, name);
        this.symbols = symbols;
    }

    private static String join(List<String> symbols) {
        StringBuilder out = new StringBuilder();
        boolean first = true;
        for (String sym : symbols) {
            if (!first) {
                out.append(",");
            }
            first = false;
            out.append(sym);
        }
        return out.toString();
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 31 * getName().hashCode();
    }

    @Override
    public void hashTo(Hasher digest) {
        super.hashTo(digest);
        for (String sym : symbols) {
            digest.putUnencodedChars(sym);
        }
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLEnumType(newAnnotations, getName(), symbols);
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        if (source.getCoreType() != getCoreType()) {
            return false;
        }
        YQLEnumType that = (YQLEnumType) source;
        for (String sym : that.symbols) {
            if (!symbols.contains(sym)) {
                return false;
            }
        }
        return true;
    }
}
