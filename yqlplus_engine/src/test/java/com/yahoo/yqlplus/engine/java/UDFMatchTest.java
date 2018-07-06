/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.api.Record;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Test matching of argument lists to UDFs.
 */
@Test
public class UDFMatchTest {
    public static class PrimitiveUnboxedFunctionHolder implements Exports {
        @Export
        public long add(long left, long right) {
            return left + right;
        }

        @Export
        public Long safeAdd(Long left, Long right) {
            if (left == null || right == null) {
                return null;
            }
            return left + right;
        }
    }

    public static class LongRecord {
        public long a;
        public Long b;

        public LongRecord(long a, Long b) {
            this.a = a;
            this.b = b;
        }
    }

    public static class LongSource implements Source {
        private List<LongRecord> records;

        public LongSource(LongRecord... records) {
            this.records = Arrays.asList(records);
        }

        @Query
        public List<LongRecord> scan() {
            return records;
        }
    }

    @Test
    public void testSource() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("func", PrimitiveUnboxedFunctionHolder.class, "source", new LongSource(
                new LongRecord(1L, null),
                new LongRecord(2L, 2L)

        )));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT a, b FROM source OUTPUT AS f1;");
        List<Record> records = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(records.size(), 2);
        Assert.assertEquals(records.get(0).get("a"), 1L);
        Assert.assertEquals(records.get(0).get("b"), null);
        Assert.assertEquals(records.get(1).get("a"), 2L);
        Assert.assertEquals(records.get(1).get("b"), 2L);
    }

    @Test(expectedExceptions = {ExecutionException.class})
    public void testFunctionNPE() throws Exception {
        // this is going to fail with a NPE with the current engine due to lack of automatic-nullification of expressions when inputs are null
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("func", PrimitiveUnboxedFunctionHolder.class, "source", new LongSource(
                new LongRecord(1L, null),
                new LongRecord(2L, 2L)

        )));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT func.add(a, b) rez FROM source OUTPUT AS f1;");
        List<Record> records = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(records.size(), 2);
        Assert.assertEquals(records.get(0).get("a"), 1L);
        Assert.assertEquals(records.get(0).get("b"), null);
        Assert.assertEquals(records.get(1).get("a"), 2L);
        Assert.assertEquals(records.get(1).get("b"), 2L);
    }

    @Test
    public void testFunctionNPESafe() throws Exception {
        // this is going to fail with a NPE with the current engine due to lack of automatic-nullification of expressions when inputs are null
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("func", PrimitiveUnboxedFunctionHolder.class, "source", new LongSource(
                new LongRecord(1L, null),
                new LongRecord(2L, 2L)

        )));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT func.add(a, b) rez FROM source WHERE b IS NOT NULL OUTPUT AS f1;");
        List<Record> records = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(records.size(), 1);
        Assert.assertEquals(records.get(0).get("rez"), 4L);
    }

    @Test
    public void testFunction() throws Exception {
        // this is going to fail with a NPE with the current engine due to lack of automatic-nullification of expressions when inputs are null
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("func", PrimitiveUnboxedFunctionHolder.class, "source", new LongSource(
                new LongRecord(1L, null),
                new LongRecord(2L, 2L)

        )));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT func.safeadd(a, b) rez FROM source OUTPUT AS f1;");
        List<Record> records = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(records.size(), 2);
        Assert.assertEquals(records.get(0).get("rez"), null);
        Assert.assertEquals(records.get(1).get("rez"), 4L);
    }

    @Test
    public void testFunctionNull() throws Exception {
        // this is going to fail with a NPE with the current engine due to lack of automatic-nullification of expressions when inputs are null
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("func", PrimitiveUnboxedFunctionHolder.class, "source", new LongSource(
                new LongRecord(1L, null),
                new LongRecord(2L, 2L)

        )));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT func.safeadd(a, b) rez FROM source WHERE b IS NULL OUTPUT AS f1;");
        List<Record> records = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(records.size(), 1);
        Assert.assertEquals(records.get(0).get("rez"), null);
    }

    @Test
    public void testFunctionWiden() throws Exception {
        // this is going to fail with a NPE with the current engine due to lack of automatic-nullification of expressions when inputs are null
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("func", PrimitiveUnboxedFunctionHolder.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT func.safeadd(1, 1) rez1, func.add(1, 1) rez2 OUTPUT AS f1;");
        List<Record> records = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(records.size(), 1);
        Assert.assertEquals(records.get(0).get("rez1"), 2L);
        Assert.assertEquals(records.get(0).get("rez2"), 2L);
    }
}
