/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

public enum GambitCoreType {
    // simple types
    VOID,
    BOOLEAN,
    INT8,
    INT16,
    INT32,
    INT64,
    TIMESTAMP,
    FLOAT32,
    FLOAT64,

    // sequences of characters and bytes
    STRING,
    BYTES,

    ANY,

    // complex types
    ERROR,
    MAP,      // <K, V>
    ARRAY,    // <V>
    SEQUENCE, // <V>
    UNION,    // <typelist>
    STRUCT,   // <open> | <field:type, ...>

    OPTIONAL, // <V>
    PROMISE,  // <V>
    FUNCTION,
    RESULT,   // <V> | error
}
