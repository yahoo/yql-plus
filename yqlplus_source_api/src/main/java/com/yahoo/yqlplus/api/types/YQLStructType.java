/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class YQLStructType extends YQLType {
    public static boolean is(YQLType type) {
        return type instanceof YQLStructType;
    }

    public static class FieldBuilder {
        private final Annotations.Builder annotations = Annotations.builder();
        private final Builder builder;
        private final String name;
        private final YQLType type;

        public FieldBuilder(Builder builder, String name, YQLType type) {
            this.builder = builder;
            this.name = name;
            this.type = type;
        }


        public Builder add() {
            YQLNamePair field = new YQLNamePair(name, type, annotations.build());
            builder.addField(field);
            return builder;
        }

        public FieldBuilder annotate(String name, int value) {
            annotations.put(name, value);
            return this;
        }

        public FieldBuilder annotate(String name, String value) {
            annotations.put(name, value);
            return this;
        }
    }

    public static class Builder {
        private final Annotations.Builder annotations = Annotations.builder();
        private String name;
        private Map<String, YQLNamePair> fields = new LinkedHashMap<>();
        private boolean closed = true;

        public Builder merge(Builder other) {
            for (YQLNamePair field : other.fields.values()) {
                addField(field);
            }
            return this;
        }

        public Builder merge(YQLStructType other) {
            for (YQLNamePair field : other.fields.values()) {
                addField(field);
            }
            return this;
        }

        public Builder addField(YQLNamePair field) {
            YQLNamePair existing = getField(field.getName());
            if (existing != null && !existing.equals(field)) {
                throw new YQLTypeException("Adding conflicting definition of field named '" + field.getName() + "'; " + existing + " != " + field);
            } else if (existing == null) {
                fields.put(field.getName(), field);
            }
            return this;
        }

        public Builder addField(String name, YQLType type) {
            addField(new YQLNamePair(name, type, Annotations.EMPTY));
            return this;
        }

        public FieldBuilder buildField(String name, YQLType type) {
            return new FieldBuilder(this, name, type);
        }

        public Builder addField(String name, YQLType type, boolean optional) {
            addField(new YQLNamePair(name, optional ? YQLOptionalType.create(type) : YQLOptionalType.deoptional(type), Annotations.EMPTY));
            return this;
        }

        public YQLNamePair getField(String name) {
            return fields.get(name);
        }

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public boolean isClosed() {
            return closed;
        }

        public Builder setClosed(boolean closed) {
            this.closed = closed;
            return this;
        }

        public YQLStructType build() {
            if (name == null) {
                Hasher digest = Hashing.md5().newHasher();
                for (YQLNamePair field : fields.values()) {
                    field.hashTo(digest);
                }
                this.name = "struct_" + digest.hash().toString();
            }
            return new YQLStructType(annotations.build(), name, fields, closed);
        }

        public Builder annotate(String name, int value) {
            annotations.put(name, value);
            return this;
        }

        public Builder annotate(String name, String value) {
            annotations.put(name, value);
            return this;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final Map<String, YQLNamePair> fields;
    private final boolean closed;

    YQLStructType(Annotations annotations, String name, Map<String, YQLNamePair> fields, boolean closed) {
        super(annotations, YQLCoreType.STRUCT, name);
        this.fields = Collections.unmodifiableMap(fields);
        this.closed = closed;
    }

    public Iterable<YQLNamePair> getFields() {
        return fields.values();
    }

    public YQLNamePair get(String name) {
        return fields.get(name);
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLStructType(newAnnotations, getName(), fields, closed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YQLStructType that = (YQLStructType) o;

        if (closed != that.closed) return false;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        int result = fields.hashCode();
        result = 31 * result + (closed ? 1 : 0);
        return result;
    }

    @Override
    public void hashTo(Hasher digest) {
        super.hashTo(digest);
        for (YQLNamePair field : fields.values()) {
            field.hashTo(digest);
        }
        digest.putBoolean(closed);
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        out.append("struct{");
        boolean first = true;
        for (YQLNamePair field : fields.values()) {
            if (!first) {
                out.append(",");
            }
            first = false;
            out.append(field.getName())
                    .append(":")
                    .append(field.getValueType().toString());
        }
        out.append("}");
        return out.toString();
    }

    @Override
    protected boolean internalAssignableFrom(YQLType source) {
        if (getCoreType() != source.getCoreType()) {
            return false;
        }
        YQLStructType that = (YQLStructType) source;
        for (YQLNamePair field : fields.values()) {
            YQLNamePair otherField = that.get(field.getName());
            if (otherField == null && field.isRequired()) {
                return false;
            }
            if (!field.isAssignableFrom(otherField)) {
                return false;
            }
        }
        return true;
    }
}
