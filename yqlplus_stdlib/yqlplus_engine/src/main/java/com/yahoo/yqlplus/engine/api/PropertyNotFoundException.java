/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.api;

import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

public class PropertyNotFoundException extends ProgramCompileException {
    public PropertyNotFoundException(String message) {
        super(message);
    }

    public PropertyNotFoundException(String message, Object... args) {
        super(message, args);
    }

    public PropertyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyNotFoundException(Throwable cause) {
        super(cause);
    }

    public PropertyNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public PropertyNotFoundException(Location sourceLocation, String message, Object... args) {
        super(sourceLocation, message, args);
    }

    public PropertyNotFoundException(Location sourceLocation, Throwable cause, String message) {
        super(sourceLocation, cause, message);
    }
}
