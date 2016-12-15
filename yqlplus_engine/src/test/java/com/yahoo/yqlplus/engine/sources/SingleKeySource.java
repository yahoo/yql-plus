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

public class SingleKeySource implements Source {

    @Query
    public Person lookup(@Key("id") String id) {
        return new Person(id, id, id.isEmpty() ? 0 : Integer.parseInt(id));
    }

    @Query
    public Person lookup(@Key("id") String id, int score) {
        return new Person(id, id, score);
    }
}
