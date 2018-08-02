/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.api;

import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

/**
 * Indicate an asset is not found.
 */
public class DependencyNotFoundException extends ProgramCompileException {
    public DependencyNotFoundException(Location sourceLocation, String message, Object... args) {
        super(sourceLocation, message, args);
    }
}
