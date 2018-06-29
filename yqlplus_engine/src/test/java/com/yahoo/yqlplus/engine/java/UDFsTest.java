/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Trace;
import com.yahoo.yqlplus.api.trace.TraceEntry;
import com.yahoo.yqlplus.api.trace.TraceRequest;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.sources.BatchKeySource;
import com.yahoo.yqlplus.engine.sources.SingleKeySource;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

public class UDFsTest {

    public static class CoolModule implements Exports {

        @Export
        public int incr(int input) {
            return input + 1;
        }

        @Export
        public String joeify(@Trace("MINE") Tracer tracer, String input) {
            return input + "joe";
        }

        @Export
        public List<Person> doubleScores(Iterable<Person> persons) {
            return ImmutableList.copyOf(Iterables.transform(persons, new Function<Person, Person>() {
                @Nullable
                @Override
                public Person apply(@Nullable Person person) {
                    return new Person(person.getId(), person.getValue(), person.getScore() * 2);
                }
            }));
        }
        
        @Export
        public static String convertString(String input) {
            return input.toUpperCase();
        }

        @Export
        public static int strlen(String input) {
            return (input != null ? input.length() : 0);
        }

        @Export
        public List<Person> removeEvenPeople(Iterable<Person> inputList) {
            List<Person> filteredRecords = new ArrayList<>();
            Iterator<Person> recordIterator = inputList.iterator();
            while (recordIterator.hasNext()) {
                Person nextRecord = recordIterator.next();
                if (nextRecord.getIidPrimitive() % 2 != 0) {
                    filteredRecords.add(nextRecord);
                }
            }
            return filteredRecords;
        }
        
        @Export
        public Long second2Millisec (Long sec) {
            if (sec == null) { 
                return null; 
            }
            else { 
                return sec * 1000; 
            }
        }
    }
	
    @Test
    public void testConvertNullLongUDF() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", NullLongSource.class, "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM (@id array<int64>); \n"
                 + "CREATE TEMP TABLE sample AS (SELECT * FROM source WHERE id IN (@id)); \n"
                 + "SELECT cool.second2Millisec(sample.id) AS convertedId FROM sample  OUTPUT as f1;";
        CompiledProgram program = compiler.compile(programStr);
        Map<String, Object> map = new HashMap<>();
        List<Long> ids = Lists.newArrayList();
        ids.add(null);
        ids.add(12L);    
        map.put("id", ids);
        ProgramResult rez = program.run(map, true);
        List<Record> result = rez.getResult("f1").get().getResult();
        assertNull(result.get(0).get("convertedId"));
        assertEquals(12000, (long)result.get(1).get("convertedId"));
    }
	
    @Test
    public void testStaticUDF() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", SingleKeySource.class,
                        "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@str string); \n"
                + "SELECT cool.convertString(@str) upperStr OUTPUT as arg;");
        ProgramResult rez = program.run(
                ImmutableMap.of("str", "cool"), true);
        List<Record> record = rez.getResult("arg").get().getResult();
        AssertJUnit.assertEquals(record.size(), 1);
        AssertJUnit.assertEquals(record.get(0).get("upperStr"), "COOL");
    }
	
	@Test
    public void testUDFs() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class, "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT cool.joeify(value) name from source WHERE id = '1' OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of(), true);
        List<Record> record = rez.getResult("f1").get().getResult();
        AssertJUnit.assertEquals(record.size(), 1);
        AssertJUnit.assertEquals(record.get(0).get("name"), "1joe");
        TraceRequest trace = rez.getEnd().get();
        boolean found = false;
        boolean foundMyTracer = false;
        for (TraceEntry entry:trace.getEntries()) {
            if (entry.getGroup().equals("PIPE") && entry.getName().equals("com.yahoo.yqlplus.engine.java.UDFsTest$CoolModule::joeify")
                    && entry.getDurationMilliseconds() > 0) {
                found = true;
            }
            if (entry.getGroup().equals("com.yahoo.yqlplus.engine.java.UDFsTest$CoolModule::joeify") && entry.getName().equals("MINE")
                    && entry.getDurationMilliseconds() > 0) {
                foundMyTracer = true;
            }
        }
        String foundMsg = "Found: " + found + " and FoundMyTracer: " + foundMyTracer;
        //Failed for no reason at one point
        AssertJUnit.assertTrue(foundMsg, found && foundMyTracer);
    }

    @Test
    public void testUDFsImportFrom() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class, "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "FROM cool IMPORT joeify;" +
                "SELECT joeify(value) name from source WHERE id = '1' OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of(), true);
        List<Record> record = rez.getResult("f1").get().getResult();
        AssertJUnit.assertEquals(record.size(), 1);
        AssertJUnit.assertEquals(record.get(0).get("name"), "1joe");
    }

    @Test
    public void testUDFsImportAlias() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class, "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "IMPORT cool.joeify AS baz;" +
                "SELECT baz(value) name from source WHERE id = '1' OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of(), true);
        List<Record> record = rez.getResult("f1").get().getResult();
        AssertJUnit.assertEquals(record.size(), 1);
        AssertJUnit.assertEquals(record.get(0).get("name"), "1joe");
    }

    @Test
    public void testUDFsImportPlain() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class, "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "IMPORT cool;" +
                "SELECT cool.joeify(value) name from source WHERE id = '1' OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of(), true);
        List<Record> record = rez.getResult("f1").get().getResult();
        AssertJUnit.assertEquals(record.size(), 1);
        AssertJUnit.assertEquals(record.get(0).get("name"), "1joe");
    }

    @Test
    public void testPipe() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class, "cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id IN ('1', '10') | cool.doubleScores OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of(), true);
        AssertJUnit.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 2), new Person("10", "10", 20)));
    }

    @Test
    public void testTemporaryTableWithUDF() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("cool", UDFsTest.CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("CREATE TEMPORARY TABLE frobaz AS (SELECT * FROM people WHERE id IN ('1', '2', '3', '4') | cool.removeEvenPeople());" +
                "SELECT id FROM frobaz OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        AssertJUnit.assertEquals(foo.size(), 2);
        AssertJUnit.assertEquals(foo.get(0).get("id"), "1");
        AssertJUnit.assertEquals(foo.get(1).get("id"), "3");
    }

    @Test
    public void testLimitOffsetUDF() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class),
            new SourceBindingModule("cool", CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@lmt int32, @off int32);" +
            "CREATE VIEW foo AS SELECT * FROM source WHERE id IN ('1', '2', '3', '4', '5');" +
            "SELECT * FROM foo LIMIT cool.incr(@lmt) OUTPUT AS f1;" +
            "SELECT * FROM foo LIMIT cool.incr(@lmt) OFFSET cool.incr(@off) OUTPUT AS f2;");
        ProgramResult rez = program.run(ImmutableMap.of("lmt", 1, "off", 1), true);
        AssertJUnit.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2)));
        AssertJUnit.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("3", "3", 3), new Person("4", "4", 4)));
    }

    public static class NullLongSource implements Source {

        @Query
        public List<SampleId> getSampleIds(@Key(value = "id", skipNull = false) List<Long> ids) {
            List<SampleId> sampleIds = Lists.newArrayList();
            for (Long id:ids) {
                sampleIds.add(new SampleId(id));
            }
            return sampleIds;
        }

        public static class SampleId {
            
            private final Long id;
            
            public SampleId(Long id) {
                this.id = id;
            }
            public Long getId() {
                return id;
            }
        }
    }
}
