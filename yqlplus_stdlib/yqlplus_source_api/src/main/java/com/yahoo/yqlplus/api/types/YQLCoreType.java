/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

public enum YQLCoreType {
    VOID,
    BOOLEAN,
    INT8,
    INT16,
    INT32,
    INT64,
    TIMESTAMP,
    FLOAT32,
    FLOAT64,
    STRING,
    BYTES,
    MAP,
    ARRAY,
    UNION,
    STRUCT,
    ENUM,
    ANY,
    OPTIONAL,

    OBJECT,   // something with properties *and* bound functions
    NAME_PAIR,    // {name, V}
    ERROR,
    SEQUENCE, // <V>

    PROMISE,  // <V>
    RESULT,   // <V> | error
}
