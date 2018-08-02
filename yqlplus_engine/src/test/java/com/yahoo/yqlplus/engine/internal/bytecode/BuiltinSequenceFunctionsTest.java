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

public class BuiltinSequenceFunctionsTest extends CompilingTestBase {
    @Test
    public void requireGroupBy() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.sequences IMPORT groupby;" +
                "SELECT * \n" +
                "    FROM s1\n" +
                "   | groupby('category', 'category', 'items')");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("category"), "hats");
        Assert.assertEquals(((List<Record>) resultRecord.get("items")).size(), 2);
    }

    @Test
    public void requireGroupByDynamic() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.sequences IMPORT groupby, row, key, rows;" +
                "SELECT * \n" +
                "    FROM s1\n" +
                "   | groupby((row).category, {'category' : key, 'items' : rows})");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("category"), "hats");
        Assert.assertEquals(((List<Record>) resultRecord.get("items")).size(), 2);
    }

    @Test
    public void requireFlatten() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 10, 'category' : 'jeeps'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.sequences IMPORT flatten, groupby, row, key, rows;" +
                "SELECT * \n" +
                "    FROM s1\n" +
                "   | groupby((row).category, rows) | flatten()");
        Assert.assertEquals(result.size(), 3);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("category"), "hats");
        resultRecord = result.get(1);
        Assert.assertEquals(resultRecord.get("category"), "hats");
        resultRecord = result.get(2);
        Assert.assertEquals(resultRecord.get("category"), "jeeps");

    }

    @Test
    public void requireDistinct() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.sequences IMPORT distinct;" +
                "SELECT * \n" +
                "    FROM s1\n" +
                "   | distinct()");
        Assert.assertEquals(result.size(), 2);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("id"), 1);
        resultRecord = result.get(1);
        Assert.assertEquals(resultRecord.get("id"), 2);
    }

    @Test
    public void requireTransform() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.sequences IMPORT row;\n" +
                "SELECT * \n" +
                "    FROM s1\n" +
                "   | yql.sequences.transform({'id' : (row).id + 10, 'name' : 'angry ' + (row).name})");
        Assert.assertEquals(result.size(), 2);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("id"), 11);
        Assert.assertEquals(resultRecord.get("name"), "angry title");
        resultRecord = result.get(1);
        Assert.assertEquals(resultRecord.get("id"), 12);
        Assert.assertEquals(resultRecord.get("name"), "angry pants");
    }

    @Test
    public void requireScatter() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.sequences IMPORT row;\n" +
                "SELECT * \n" +
                "    FROM s1\n" +
                "   | yql.sequences.scatter({'id' : (row).id + 10, 'name' : 'angry ' + (row).name}) | yql.sequences.filter((row).id <= 11) ");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("id"), 11);
        Assert.assertEquals(resultRecord.get("name"), "angry title");
    }
}
