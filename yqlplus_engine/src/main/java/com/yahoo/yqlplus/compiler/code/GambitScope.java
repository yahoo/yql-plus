/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.api.types.YQLType;

import java.io.IOException;

public interface GambitScope extends GambitTypes {
    void build() throws IOException, ClassNotFoundException;

    Class<?> getObjectClass(ObjectBuilder target) throws ClassNotFoundException;

    YQLType createYQLType(TypeWidget setType);

    void addClass(Class<?> clazz);
}
