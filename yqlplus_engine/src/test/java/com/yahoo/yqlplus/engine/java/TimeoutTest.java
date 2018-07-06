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
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.compiler.runtime.YQLRuntimeException;
import com.yahoo.yqlplus.engine.sources.TimeoutSource;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeoutTest {

    @Test(enabled = false)
    public void testTimeoutWorking() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(10) OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of());
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
    }

    @Test(enabled = false)
    public void testTimeoutFailing() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(10) TIMEOUT 5 OUTPUT as f1;");
        try {
            List<Person> rez = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
            Assert.fail("should fail with a timeout");
        } catch (YQLRuntimeException | ExecutionException e) {
            Assert.assertTrue(e.getMessage().contains("Timeout"));
        }
    }

    @Test(enabled = false)
    public void testTimeoutCheck() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(8, false) TIMEOUT 5 OUTPUT as f1;");
        try {
            List<Person> rez = program.run(ImmutableMap.of()).getResult("f1").get().getResult();
            Assert.fail("should time out");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test(enabled = false)
    public void testTimeoutCheckWorks() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(5, true) TIMEOUT 5 OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of());
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
    }

    @Test(enabled = false)
    public void testProgramTimeout() throws Exception {
        // Not overriding default program timeout
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(20) OUTPUT as f1;");
        ProgramResult rez = program.run(ImmutableMap.of());
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
        
        // Overriding default program timeout
        injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        compiler = injector.getInstance(YQLPlusCompiler.class);
        program = compiler.compile("SELECT * FROM timers(20) OUTPUT as f1;");
        try {
            program.run(ImmutableMap.of(), TaskContext.builder().withTimeout(5, TimeUnit.MILLISECONDS).build()).getResult("f1").get().getResult();
            Assert.fail("should time out");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TimeoutException);
            Assert.assertTrue(e.getMessage().contains("Timeout"));
        }
    }

    // --- now the join version of all ---


    @Test(enabled = false)
    public void testTimeoutFailingJoin() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(10) t1 JOIN timers(10) t2 ON t1.id = t2.id TIMEOUT 5 OUTPUT as f1;");
        try {
            program.run(ImmutableMap.of()).getResult("f1").get().getResult();
            Assert.fail("should time out");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test(enabled = false)
    public void testTimeoutCheckJoin() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM timers(5, false) t1 JOIN timers(5, false) t2 ON t1.id = t2.id TIMEOUT 5 OUTPUT as f1;");
        try {
            program.run(ImmutableMap.of()).getResult("f1").get().getResult();
            Assert.fail("should time out");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test(expectedExceptions = {ProgramCompileException.class}, expectedExceptionsMessageRegExp = "<string>:L1:24 no viable alternative at input 'SELECT \\* FROM timers\\(10 JOIN'")
    public void testParseException() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        // bad syntax
        compiler.compile("SELECT * FROM timers(10 JOIN timers(10) t2 ON t1.id = t2.id TIMEOUT 5 OUTPUT as f1;");
    }

    @Test(expectedExceptions = {ProgramCompileException.class}, expectedExceptionsMessageRegExp = "<string>:L1:83 mismatched input '.' expecting ';'")
    public void testParseException2() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("timers", TimeoutSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        // bad syntax
        compiler.compile("SELECT * FROM timers(10) JOIN timers(10) t2 ON t1.id = t2.id TIMEOUT 5 OUTPUT as f1.bar;");
    }
}
