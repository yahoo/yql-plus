/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.Person;

public class ArraySyntaxTestSource implements Source {
	
    @Query
    public ArraySyntaxTestRecord scan() {
        return new ArraySyntaxTestRecord();
    }

    public static class ArraySyntaxTestRecord {
        public List<Person> people = ImmutableList.of(new Person("2", "person", 1));

        public List<Person> getDudes() {
            return ImmutableList.of(new Person("1", "dude", 10));
        }
    }
}
