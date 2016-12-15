/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.annotations;

import com.google.inject.ScopeAnnotation;

import javax.inject.Scope;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicate a given object is scoped to program execution-time.
 */
@Target({TYPE, METHOD})
@Retention(RUNTIME)
@Scope
@Documented
@ScopeAnnotation
public @interface ExecuteScoped {
}