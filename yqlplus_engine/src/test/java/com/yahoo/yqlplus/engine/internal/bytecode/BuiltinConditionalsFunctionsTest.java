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

public class BuiltinConditionalsFunctionsTest extends CompilingTestBase {
    @Test
    public void requireCase() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.conditionals IMPORT case;" +
                "SELECT case(s1.id = 1, 'hairy', 'eyeball') status \n" +
                "  FROM s1");
        Assert.assertEquals(result.size(), 2);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("status"), "hairy");
        resultRecord = result.get(1);
        Assert.assertEquals(resultRecord.get("status"), "eyeball");
    }


    @Test
    public void requireCoalesce() throws Exception  {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : NULL}]");
        List<Record> result = runQueryProgram("" +
                "FROM yql.conditionals IMPORT coalesce;" +
                "SELECT coalesce(s1.category, 'default') category \n" +
                "  FROM s1");
        Assert.assertEquals(result.size(), 2);
        Record resultRecord = result.get(0);
        Assert.assertEquals(resultRecord.get("category"), "hats");
        resultRecord = result.get(1);
        Assert.assertEquals(resultRecord.get("category"), "default");

    }

}
