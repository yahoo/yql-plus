/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.parser;

/**
 * A pointer to a location in a CQL source program.
 */
public final class Location {
    public static final Location NONE = new Location("<none>", -1, 0);

    private final String programName;
    private final int lineNumber;
    private final int characterOffset;

    public Location(String programName, int lineNumber, int characterOffset) {
        this.programName = programName;
        this.lineNumber = lineNumber;
        this.characterOffset = characterOffset;
    }


    public String getProgramName() {
        return programName;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public int getCharacterOffset() {
        return characterOffset;
    }


    @Override
    public String toString() {
        if (programName != null) {
            return programName + ":L" + lineNumber + ":" + characterOffset;
        } else {
            return "L" + lineNumber + ":" + characterOffset;
        }
    }
}
