/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import org.testng.Assert;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.TimeoutMilliseconds;
import com.yahoo.yqlplus.engine.java.Person;

public class TimeoutSource implements Source {
    @Query
    public Person scan(int delay) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
        return new Person("1", "1", 1);
    }

    @Query
    public Person scan(@TimeoutMilliseconds long remainingTime, int expected, boolean work) {
        Assert.assertTrue(remainingTime < expected);
        if (!work) {
        	try {
        		Thread.sleep(remainingTime + 10);
        	} catch (InterruptedException e) {
        	}
        }
        return new Person("1", "1", 1);
    }
}
