/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

import com.google.common.collect.Lists;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.Person;

public class PersonListMakeSource implements Source {
    @Query
    public List<Person> lookup(@Key("id") List<String> ids, int low, int high) {
        List<Person> persons = Lists.newArrayList();
        for (String id:ids) {
            persons.add(new Person(id, id, Integer.parseInt(id)));
        }
        return (List<Person>) (Object)Lists.newArrayList(persons.stream().filter(person -> person.getIidPrimitive() > low && person.getIidPrimitive() < high).toArray());
    }
}
