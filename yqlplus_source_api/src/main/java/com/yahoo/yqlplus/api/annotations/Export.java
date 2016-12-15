/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.annotations;

import java.lang.annotation.*;

/**
 * Indicate a given method or field should be visible to a YQL+ program in an expression.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
@Documented
public @interface Export {
    /**
     * Indicate a name for this export. Will default to the member name.
     */
    String value() default "";
}
