/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.annotations;

import java.lang.annotation.*;

/**
 * Indicate a parameter to communicate the timeout in milliseconds for a Source or Transform.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
@Documented
public @interface TimeoutMilliseconds {

}
