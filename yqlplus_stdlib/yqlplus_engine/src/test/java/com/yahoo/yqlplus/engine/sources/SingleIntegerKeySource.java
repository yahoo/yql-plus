/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.IntegerPerson;

public class SingleIntegerKeySource implements Source {

    @Query
    public IntegerPerson lookup(@Key("id") Integer id) {
        if (null == id) {
            throw new IllegalArgumentException("Null id");
        }
        return new IntegerPerson(id, id, id.intValue());
    }
}
