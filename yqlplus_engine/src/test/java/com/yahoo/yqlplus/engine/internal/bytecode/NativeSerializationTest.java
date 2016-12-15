/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.java.PrimitiveRecord;
import com.yahoo.yqlplus.engine.java.PrimitiveSource;
import com.yahoo.yqlplus.engine.java.SourceBindingModule;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

// Disable the tests in this test class for now
// These tests are used for gateway implementation
public class NativeSerializationTest extends CompilingTestBase {

    public static final MappingJsonFactory MAPPING_JSON_FACTORY = new MappingJsonFactory();

    @Test(enabled = false)
    public void requireJsonNativeEncoding() throws Exception {
        defineView("s1", "EVALUATE [{'id' : 1, 'name' : 'title', 'category' : 'hats'}, {'id' : 2, 'name' : 'pants', 'category' : 'hats'}]");
        ByteArrayOutputStream outputStream = runQueryProgramSerialized(NativeEncoding.JSON, "SELECT * FROM s1 WHERE id = 1");
        Assert.assertEquals((JsonNode)MAPPING_JSON_FACTORY.createParser(outputStream.toByteArray()).readValueAsTree(),
                (JsonNode)MAPPING_JSON_FACTORY.createParser("[{\"id\" : 1, \"name\" : \"title\", \"category\" : \"hats\"}]").readValueAsTree());
    }

    @Test(enabled = false)
    public void requireJsonNativePrimitives() throws Exception {
        ByteArrayOutputStream outputStream = runQueryProgramSerialized(NativeEncoding.JSON, "SELECT * FROM psource",
                new SourceBindingModule("psource", new PrimitiveSource(ImmutableList.of(new PrimitiveRecord('a', (byte)1, (short)2, 3, 4, true, 1.0f, 1.0))))
                );
        Assert.assertEquals((JsonNode)MAPPING_JSON_FACTORY.createParser(outputStream.toByteArray()).readValueAsTree(),
                (JsonNode)MAPPING_JSON_FACTORY.createParser("[{\"p_char\":\"a\",\"b_char\":\"a\",\"p_byte\":1,\"b_byte\":1,\"p_short\":2,\"b_short\":2,\"p_int\":3,\"b_int\":3,\"p_long\":4,\"b_long\":4,\"p_boolean\":false,\"p_float\":1.0,\"b_float\":1.0,\"p_double\":1.0,\"b_double\":1.0}]").readValueAsTree());
    }


    public static class SomeRecord {
        private final String name;

        public SomeRecord(String name) {
            this.name = name;
        }

        @JsonIgnore
        public String getName() {
            return name;
        }
    }

    public static class SomeRecordSource implements Source {
        private final SomeRecord record;

        public SomeRecordSource(SomeRecord record) {
            this.record = record;
        }

        @Query
        public SomeRecord scan() {
            return record;
        }
    }

    /**
     * This one verifies that the "view" seen by the native serializer is the struct-view (same as one can evaluate expressions from) and not being sent directly to Jackson.
     */
    @Test(enabled = false)
    public void requireNativeIsNotJackson() throws Exception {
        ByteArrayOutputStream outputStream = runQueryProgramSerialized(NativeEncoding.JSON, "SELECT * FROM somesource",
                new SourceBindingModule("somesource",new SomeRecordSource(new SomeRecord("joe")))
        );
        Assert.assertEquals((JsonNode)MAPPING_JSON_FACTORY.createParser(outputStream.toByteArray()).readValueAsTree(),
                (JsonNode)MAPPING_JSON_FACTORY.createParser("[{\"name\":\"joe\"}]").readValueAsTree());
    }
}
