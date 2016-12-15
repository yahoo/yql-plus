/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public class YQLNamePair extends YQLNamedValueType {
    public static class NamePairBuilder {
        private final Annotations.Builder annotations = Annotations.builder();
        private String name;
        private YQLType type;

        public NamePairBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public NamePairBuilder setType(YQLType type) {
            this.type = type;
            return this;
        }

        public NamePairBuilder annotate(String name, int value) {
            annotations.put(name, value);
            return this;
        }

        public NamePairBuilder annotate(String name, String value) {
            annotations.put(name, value);
            return this;
        }

        public YQLNamePair build() {
            return new YQLNamePair(name, type, annotations.build());
        }
    }


    YQLNamePair(String name, YQLType type, Annotations annotations) {
        super(annotations, YQLCoreType.NAME_PAIR, name, type);
    }

    @Override
    public YQLType withAnnotations(Annotations newAnnotations) {
        return new YQLNamePair(getName(), getValueType(), newAnnotations);
    }
}
