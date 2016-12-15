/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;


import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

import java.lang.reflect.Type;

public interface TypeAdaptingWidget {
    boolean supports(Class<?> clazzType);

    TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type);
}
