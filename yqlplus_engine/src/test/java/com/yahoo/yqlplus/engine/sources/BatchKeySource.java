/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.Person;

import java.util.List;

public class BatchKeySource implements Source {
    @Query
    public Iterable<Person> lookupAll(@Key("id") List<String> ids) {
        return Iterables.transform(ids, new Function<String, Person>() {
            @Override
            public Person apply(String input) {
                return new Person(input, input, Integer.parseInt(input));
            }
        });
    }

    @Query
    public Iterable<Person> lookupAll(@Key("id") List<String> ids, final int score) {
        return Iterables.transform(ids, new Function<String, Person>() {
            @Override
            public Person apply(String input) {
                return new Person(input, input, score);
            }
        });
    }
}
