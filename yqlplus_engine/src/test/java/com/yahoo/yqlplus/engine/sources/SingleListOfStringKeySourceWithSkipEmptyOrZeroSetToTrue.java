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
import java.util.ArrayList;
import java.util.List;

public class SingleListOfStringKeySourceWithSkipEmptyOrZeroSetToTrue implements Source {

    @Query
    public Iterable<Person> lookup(@Key(value = "id", skipEmptyOrZero = true) List<String> ids) {
        if (null == ids || ids.isEmpty()) {
            throw new IllegalArgumentException("Null or empty id");
        }
        List<Person> persons = new ArrayList<>(ids.size());
        for (String id : ids) {
            persons.add(new Person(id, id, Integer.parseInt(id)));
        }
        return persons;
    }
}
