/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.parser;

public class ProgramCompileException extends RuntimeException {
    private Location sourceLocation;

    public ProgramCompileException(String message) {
        super(message);
    }

    public ProgramCompileException(String message, Object... args) {
        super(formatMessage(message, args));
    }

    private static String formatMessage(String message, Object... args) {
        return args == null ? message : String.format(message, args);
    }

    public ProgramCompileException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProgramCompileException(Throwable cause) {
        super(cause);
    }

    public ProgramCompileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    private static String compose(Location location, String message, Object... args) {
        if (args != null) {
            message = String.format(message, args);
        }
        if (location != null) {
            return location + " " + message;
        } else {
            return message;
        }
    }

    public ProgramCompileException(Location sourceLocation, String message, Object... args) {
        super(compose(sourceLocation, message, args));
        this.sourceLocation = sourceLocation;
    }

    public ProgramCompileException(Location sourceLocation, Throwable cause, String message) {
        super(compose(sourceLocation, message), cause);
        this.sourceLocation = sourceLocation;
    }

    public Location getSourceLocation() {
        return sourceLocation;
    }
}
