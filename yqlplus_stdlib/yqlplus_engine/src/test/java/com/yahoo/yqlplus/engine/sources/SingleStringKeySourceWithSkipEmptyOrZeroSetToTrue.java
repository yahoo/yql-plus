/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.Person;

public class SingleStringKeySourceWithSkipEmptyOrZeroSetToTrue implements Source {

    @Query
    public Person lookup(@Key(value = "id", skipEmptyOrZero = true) String id) {
        if (null == id || id.isEmpty()) {
            throw new IllegalArgumentException("Null or empty id");
        }
        return new Person(id, id, Integer.parseInt(id));
    }
}
