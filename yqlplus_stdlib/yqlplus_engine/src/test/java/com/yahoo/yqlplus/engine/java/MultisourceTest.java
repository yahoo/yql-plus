/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.internal.bytecode.types.JVMTypes;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;

public class MultisourceTest {

    // this is a little contrived
    public static class Resource {
        public String id;
        public String name;
        public float score;
    }

    public static class ResourceSource implements Source {
        static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();
        @Query
        public List<Resource> scan(String resourceName) throws Exception {
            InputStream data = ResourceSource.class.getResourceAsStream(resourceName);
            JsonParser parser = JSON_FACTORY.createParser(data);
            return  parser.readValueAs(new TypeReference<List<JsonNode>>() {
                @Override
                public Type getType() {
                    return JVMTypes.createParameterizedType(List.class, Resource.class);
                }
            });
        }

    }


    @Test
    public void testMultisource() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("resource", ResourceSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "CREATE VIEW movies AS SELECT * FROM resource('movies.json');" +
                "CREATE VIEW hats AS SELECT * FROM resource('hats.json');" +
                "" +
                "SELECT * " +
                "FROM SOURCES movies, hats " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Resource> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 5);
    }
}
