/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class FilterPlanningTest {

    public static class MagazineSource implements Source {
        int call_scan;
        int call_idx;

        @Query
        public  List<Person> scan() {
            ++call_scan;
            return ImmutableList.of(new Person("1", "hodor", 100));
        }

        @Query
        public  List<Person> lookup(@Key("id") String id) {
            ++call_idx;
            return ImmutableList.of();
        }
    }

    @Test
    public void requireNonRowDependantOptimizationScan() throws Exception {
        MagazineSource src = new MagazineSource();
        Injector injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("failing_source", src)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@call boolean);" +
                "SELECT * FROM failing_source WHERE @call OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.of("call", false), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 0);
        Assert.assertEquals(src.call_idx, 0, "Expected no calls to lookup()");
        Assert.assertEquals(src.call_scan, 0, "Expected no calls to scan()");
    }

    @Test
    public void requireNonRowDependantInvokedScan() throws Exception {
        MagazineSource src = new MagazineSource();
        Injector injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("failing_source", src)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@call boolean);" +
                "SELECT * FROM failing_source WHERE @call OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.of("call", true), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Assert.assertEquals(src.call_idx, 0, "Expected no calls to lookup()");
        Assert.assertEquals(src.call_scan, 1, "Expected 1 call to scan()");
    }

    @Test
    public void requireNonRowDependantOptimizationIndexed() throws Exception {
        MagazineSource src = new MagazineSource();
        Injector injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("failing_source", src)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@call boolean);" +
                "SELECT * FROM failing_source WHERE @call AND id = '1' OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.of("call", false), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 0);
        Assert.assertEquals(src.call_idx, 0, "Expected no calls to lookup()");
        Assert.assertEquals(src.call_scan, 0, "Expected no calls to scan()");
    }

    @Test
    public void requireNonRowDependantInvokedIndexed() throws Exception {
        MagazineSource src = new MagazineSource();
        Injector injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("failing_source", src)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@call boolean);" +
                "SELECT * FROM failing_source WHERE @call AND id = '1' OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.of("call", true), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 0);
        Assert.assertEquals(src.call_idx, 1, "Expected 1 call to lookup()");
        Assert.assertEquals(src.call_scan, 0, "Expected no calls to scan()");
    }

    public static class Decider implements Exports {
        @Export
        public List<String> modules(int choice) {
            switch(choice) {
                case 1:
                    return ImmutableList.of("1", "3");
                case 2:
                    return ImmutableList.of("3", "4", "5");
                default:
                    return ImmutableList.of("1", "2", "3", "4", "5");
            }
        }
    }

    public static class PersonNamer implements Source {

        // standing in for multiple sources
        @Query
        public  List<Person> scan(String choice) {
            if("2".equals(choice)) {
                throw new IllegalArgumentException("don't call me with '2'");
            }
            return ImmutableList.of(
                    new Person(choice + ".1", "name", 100),
                    new Person(choice + ".2", "name", 100),
                    new Person(choice + ".3", "name", 100)
            );
        }
    }

    @Test
    public void requireFilteringUdf() throws Exception {
        Injector injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("namer", PersonNamer.class,
                        "decider", Decider.class)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@choice int32);" +
                "CREATE TEMPORARY TABLE modules AS ( EVALUATE decider.modules(@choice) );" +
                "CREATE TEMPORARY TABLE articles AS ( " +
                "    SELECT * FROM namer('1') WHERE '1' IN (SELECT m FROM @modules m) " +
                "MERGE " +
                "    SELECT * FROM namer('2') WHERE '2' IN (SELECT m FROM @modules m) " +
                "MERGE " +
                "    SELECT * FROM namer('3') WHERE '3' IN (SELECT m FROM @modules m) " +
                "MERGE " +
                "    SELECT * FROM namer('4') WHERE '4' IN (SELECT m FROM @modules m) ); " +
                "SELECT * FROM articles ORDER BY id OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.of("choice", 1), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo, ImmutableList.of(
                new Person("1.1", "name", 100),
                new Person("1.2", "name", 100),
                new Person("1.3", "name", 100),
                new Person("3.1", "name", 100),
                new Person("3.2", "name", 100),
                new Person("3.3", "name", 100))
        );
    }

    public static class PersonNamer1 implements Source {

        @Query
        public  List<Person> scan() {
            return ImmutableList.of(
                    new Person("1.1", "name", 100),
                    new Person("1.2", "name", 100),
                    new Person("1.3", "name", 100)
            );
        }
    }

    public static class PersonNamer2 implements Source {

        @Query
        public  List<Person> scan() {
            throw new IllegalArgumentException("Don't call me");
        }
    }

    public static class PersonNamer3 implements Source {

        @Query
        public  List<Person> scan() {
            return ImmutableList.of(
                    new Person("3.1", "name", 100),
                    new Person("3.2", "name", 100),
                    new Person("3.3", "name", 100)
            );
        }
    }

    public static class PersonNamer4 implements Source {

        @Query
        public  List<Person> scan() {
            return ImmutableList.of(
                    new Person("4.1", "name", 100),
                    new Person("4.2", "name", 100),
                    new Person("4.3", "name", 100)
            );
        }
    }

    @Test
    public void testSourceNotInvokedIfNotRowDependentFilterExpressionEvalsToFalse() throws Exception {
        Injector injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("namer1", PersonNamer1.class, "namer2", PersonNamer2.class, "namer3", PersonNamer3.class,
                        "namer4", PersonNamer4.class, "decider", Decider.class)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@choice int32);" +
                "CREATE TEMPORARY TABLE modules AS ( EVALUATE decider.modules(@choice) );" +
                "CREATE TEMPORARY TABLE articles AS ( " +
                "    SELECT * FROM namer1 WHERE '1' IN (SELECT m FROM @modules m) " +
                "MERGE " +
                "    SELECT * FROM namer2 WHERE '2' IN (SELECT m FROM @modules m) " +
                "MERGE " +
                "    SELECT * FROM namer3 WHERE '3' IN (SELECT m FROM @modules m) " +
                "MERGE " +
                "    SELECT * FROM namer4 WHERE '4' IN (SELECT m FROM @modules m) ); " +
                "SELECT * FROM articles ORDER BY id OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.of("choice", 1), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo, ImmutableList.of(
                new Person("1.1", "name", 100),
                new Person("1.2", "name", 100),
                new Person("1.3", "name", 100),
                new Person("3.1", "name", 100),
                new Person("3.2", "name", 100),
                new Person("3.3", "name", 100))
        );
    }
}
