/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class ErrorSource implements Source {
    @Query
    public Person getPersons() {
        Person person = new Person();
        person.age = new Integer("a,b");
        return person;
    }

    public static class Person {
        public String name;
        public int age;    
    }
}
