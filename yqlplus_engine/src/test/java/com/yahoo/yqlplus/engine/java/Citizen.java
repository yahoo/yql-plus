/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

public class Citizen {

    private String id;
    private String country;

    public Citizen(String id, String country) {
        this.id = id;
        this.country = country;
    }

    public String getId() {
        return id;
    }

    public String getCountry() {
        return country;
    }
}
