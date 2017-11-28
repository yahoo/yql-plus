/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.plan.types.base.StructBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class BuiltinRecordFunctionsTest extends CompilingTestBase {
    @Test
    public void requireDynamicMerge() throws Exception {
        defineView("s1", "EVALUATE [{'record' : {'id' : 1, 'hodor' : 'hodor'}, 'link' : {'href' : 'a', 'text' : 'hi'}}]");
        List<Record> result = runQueryProgram("" +
                "SELECT yql.records.merge(record, link) datum, yql.records.map(record, link) datum_map \n" +
                "    FROM s1");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = (Record) result.get(0).get("datum");
        Assert.assertEquals(resultRecord.get("id"), 1);
        Assert.assertEquals(resultRecord.get("hodor"), "hodor");
        Assert.assertEquals(resultRecord.get("href"), "a");
        Assert.assertEquals(resultRecord.get("text"), "hi");
        resultRecord = (Record) result.get(0).get("datum_map");
        Assert.assertEquals(resultRecord.get("id"), 1);
        Assert.assertEquals(resultRecord.get("hodor"), "hodor");
        Assert.assertEquals(resultRecord.get("href"), "a");
        Assert.assertEquals(resultRecord.get("text"), "hi");
    }

    public static class User {
        private final String userId;
        private final String userName;

        public User(String userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }

        public String getUserId() {
            return userId;
        }

        public String getUserName() {
            return userName;
        }
    }

    public static class Link {
        private final String id;
        private final String name;

        public Link(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    public static class Favorite {
        private final String favoriteId;
        private final User user;
        private final Link link;

        public Favorite(String favoriteId, User user, Link link) {
            this.favoriteId = favoriteId;
            this.user = user;
            this.link = link;
        }

        public String getFavoriteId() {
            return favoriteId;
        }

        public User getUser() {
            return user;
        }

        public Link getLink() {
            return link;
        }
    }


    public static class RecordsSource implements Source {
        @Query
        public List<Favorite> scan() {
            return ImmutableList.of(new Favorite("fav1", new User("user1", "joe"), new Link("1", "target")));
        }
    }

    @Test
    public void requireStaticMerge() throws Exception {
        defineSource("users.static", RecordsSource.class);
        List<Record> result = runQueryProgram("" +
                "SELECT yql.records.merge(user, link) datum, yql.records.map(user, link) datum_map \n" +
                "    FROM users.static");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = (Record) result.get(0).get("datum");
        Assert.assertEquals(resultRecord.get("id"), "1");
        Assert.assertEquals(resultRecord.get("name"), "target");
        Assert.assertEquals(resultRecord.get("userId"), "user1");
        Assert.assertEquals(resultRecord.get("userName"), "joe");
        Assert.assertTrue(resultRecord instanceof StructBase);
        resultRecord = (Record) result.get(0).get("datum_map");
        Assert.assertEquals(resultRecord.get("id"), "1");
        Assert.assertEquals(resultRecord.get("name"), "target");
        Assert.assertEquals(resultRecord.get("userId"), "user1");
        Assert.assertEquals(resultRecord.get("userName"), "joe");
        Assert.assertTrue(resultRecord instanceof Map);
    }

    @Test
    public void requireAutomaticDynamicMerge() throws Exception {
        defineSource("users.static", RecordsSource.class);
        defineView("s1", "EVALUATE [{'id' : '1', 'record' : {'id' : '1', 'hodor' : 'hodor'}, 'link' : {'href' : 'a', 'text' : 'hi'}}]");
        List<Record> result = runQueryProgram("" +
                "SELECT yql.records.merge(sx.user, s1.link) datum \n" +
                "    FROM users.static sx JOIN s1 ON s1.id = sx.link.id");
        Assert.assertEquals(result.size(), 1);
        Record resultRecord = (Record) result.get(0).get("datum");
        Assert.assertEquals(resultRecord.get("href"), "a");
        Assert.assertEquals(resultRecord.get("text"), "hi");
        Assert.assertEquals(resultRecord.get("userId"), "user1");
        Assert.assertEquals(resultRecord.get("userName"), "joe");
        Assert.assertTrue(resultRecord instanceof Map);
    }
}
