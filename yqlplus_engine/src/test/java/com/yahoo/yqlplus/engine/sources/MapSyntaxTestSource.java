/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.Person;

public class MapSyntaxTestSource implements Source {
    @Query
    public MapSyntaxTestRecord scan() {
        return new MapSyntaxTestRecord();
    }
    
    public static class MapSyntaxTestRecord {
        public Map<String, Person> people = ImmutableMap.of("joe", new Person("2", "person", 1));

        public Map<String, Person> getDudes() {
            return ImmutableMap.of("dude", new Person("1", "dude", 10));
        }
    }

}
