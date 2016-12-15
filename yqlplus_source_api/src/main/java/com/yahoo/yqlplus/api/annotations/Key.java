/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.annotations;

import java.lang.annotation.*;

/**
 * Indicate a parameter to communicate a Key. Can apply to a List or individual value of either
 * String or integer type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
@Documented
public @interface Key {
    String value();
    boolean skipEmptyOrZero() default false;
    boolean skipNull() default true;
}
