/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.logical;

public enum SequenceOperatorType {
    TRANSFORM, // transform one source to another
    SOURCE,    // a source
    JOIN,      // merge multiple sources together
    CHOICE,
    MODIFY     // ???
}
