/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.engine.api.Record;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Several JOIN scenarios.
 */
public class JoinCompilerTest extends CompilingTestBase {
    @Test
    public void requireSimpleJoin() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title'}]");
        defineView("s2", "EVALUATE [{'id' : 1, 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("SELECT *\n" +
                "    FROM s1\n" +
                "    JOIN s2 ON s1.id = s2.id");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = result.get(0);
        Assert.assertEquals(((Record)resultRecord.get("s1")).get("id"), 1);
        Assert.assertEquals(((Record)resultRecord.get("s1")).get("name"), "title");
        Assert.assertEquals(((Record)resultRecord.get("s2")).get("id"), 1);
        Assert.assertEquals(((Record)resultRecord.get("s2")).get("category"), "hats");
    }

    @Test
    public void requireSimpleJoinWithProjection() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title'}]");
        defineView("s2", "EVALUATE [{'id' : 1, 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("SELECT s1.id, s2.category name\n" +
                "    FROM s1\n" +
                "    JOIN s2 ON s1.id = s2.id");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("id"), 1);
        Assert.assertEquals(resultRecord.get("name"), "hats");
    }



    @Test
    public void requireCompoundJoin() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}]");
        defineView("s2", "EVALUATE [{'id' : 1, 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("SELECT s1.id, s2.category name\n" +
                "    FROM s1\n" +
                "    JOIN s2 ON s1.id = s2.id AND s1.category = s2.category");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("id"), 1);
        Assert.assertEquals(resultRecord.get("name"), "hats");
    }

    @Test
    public void requireCompoundJoinLeftFilter() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}]");
        defineView("s2", "EVALUATE [{'id' : 1, 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("SELECT *\n" +
                "    FROM s1\n" +
                "    JOIN s2 ON s1.id = s2.id AND s1.category = s2.category" +
                "   WHERE s1.category = 'pants'");
        Assert.assertEquals(result.size(), 0);
    }

    // SELECT *
    // FROM s1
    // JOIN s2 ON s1.id = s2.id AND s1.category = s2.category
    // WHERE s1.category = 'pants' OR s1.category = 'shirt';

    // SELECT *
    // FROM s1
    // JOIN s2 ON s1.id = s2.id AND s1.category = s2.category
    // WHERE s1.category = 'pants' OR s1.category = 'shirt' AND s2.priority = 1;

    // SELECT *
    // FROM s1
    // JOIN s2 ON s1.id = s2.id AND s1.category = s2.category
    // WHERE s1.category = 'pants' OR s1.category = 'shirt' AND s2.priority + s1.priority > 2;

}
