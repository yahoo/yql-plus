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
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ProgramArgumentTest {
	private static final boolean DEBUG_DUMP = false;
	
	@Test
    public void testArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "PROGRAM (@a string);" +
                "SELECT * FROM innersource WHERE id = @a OUTPUT AS b1;");
        ProgramResult myResult = program.run(ImmutableMap.of("a", "1"));
        YQLResultSet b1 = myResult.getResult("b1").get();
        List<Person> b1r = b1.getResult();
        Assert.assertEquals(b1r.size(), 1);
        Assert.assertEquals(b1r.get(0).getId(), "1");
        dumpDebugInfo(program, myResult);
    }

    @Test
    public void testRecursiveResolve() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "PROGRAM (@a int32);" +
                "SELECT id FROM innersource WHERE iid = @a OUTPUT AS b1;" +
                "SELECT id FROM b1 OUTPUT AS b2;");
        ProgramResult myResult = program.run(ImmutableMap.of("a", 1));
        YQLResultSet b1 = myResult.getResult("b1").get();
        List<Record> b1r = b1.getResult();
        Assert.assertEquals(b1r.size(), 1);
        Assert.assertEquals(b1r.get(0).get("id"), "1");
        List<Record> b2r = b1.getResult();
        Assert.assertEquals(b2r.size(), 1);
        Assert.assertEquals(b2r.get(0).get("id"), "1");
        dumpDebugInfo(program, myResult);
    }

    @Test
    public void testInt32Argument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "PROGRAM (@a int32);" +
                "SELECT * FROM innersource WHERE iid = @a OUTPUT AS b1;");
        ProgramResult myResult = program.run(ImmutableMap.of("a", 1));
        YQLResultSet b1 = myResult.getResult("b1").get();
        List<Person> b1r = b1.getResult();
        Assert.assertEquals(b1r.size(), 1);
        Assert.assertEquals(b1r.get(0).getId(), "1");
        dumpDebugInfo(program, myResult);
    }

    @Test
    public void testInt32ArgumentUnboxed() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("" +
                "PROGRAM (@a int32);" +
                "SELECT * FROM innersource WHERE iidprimitive = @a OUTPUT AS b1;");
        ProgramResult myResult = program.run(ImmutableMap.of("a", 1));
        YQLResultSet b1 = myResult.getResult("b1").get();
        List<Person> b1r = b1.getResult();
        Assert.assertEquals(b1r.size(), 1);
        Assert.assertEquals(b1r.get(0).getId(), "1");
        dumpDebugInfo(program, myResult);
    }
    
    @Test
    public void testArrayArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@ids array<string>, @codes array<int32>);" +
                "SELECT * FROM people WHERE id IN (@ids) OUTPUT AS f1;" +
                "SELECT * FROM people WHERE score in (@codes) OUTPUT AS f2;");
        ProgramResult rez = program.run(ImmutableMap.of("ids", ImmutableList.of("1", "2", "3"), "codes", ImmutableList.of(0, 1, 2)));
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2)));
        Assert.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("1", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2)));
    }
    
    @Test
    public void testArgumentMissAlpha() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String errorMessage = null;
        try {
            CompiledProgram program = compiler.compile("" +
                "PROGRAM (@a int32);" +
                "SELECT * FROM innersource WHERE iid = a OUTPUT AS b1;");
        } catch (ProgramCompileException e) {
            errorMessage = e.getMessage();
        }
        Assert.assertTrue(errorMessage.contains("L1:57"));
    }
    
    @Test
    public void testOverrideDefaultValues() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM (@strArg array<string> = ['Bob Bill']," +
                                "@intArg array<int32>" +
                                " = [1,2,3]); " +
                                "SELECT @strArg, @intArg OUTPUT AS program_arguments;";
         CompiledProgram program = compiler.compile(programStr);    
         ProgramResult myResult = program.run(ImmutableMap.of("intArg", ImmutableList.of(99, 88)));
         Object intArg = ((Record) ((List<Object>)myResult.getResult("program_arguments").get().getResult()).get(0)).get("intArg");
         Assert.assertEquals(intArg, ImmutableList.of(99, 88));
         Object strArg = ((Record) ((List<Object>)myResult.getResult("program_arguments").get().getResult()).get(0)).get("strArg");
        Assert.assertEquals(strArg, ImmutableList.of("Bob Bill"));
    } 
    
    @Test
    public void testSignedDouble() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM (@doubleArg double=-0.1," +
                                "@intArg array<int32>" +
                                " = [1,2,3]); " +
                                "SELECT @doubleArg, @intArg OUTPUT AS program_arguments;";
         CompiledProgram program = compiler.compile(programStr);
         ProgramResult myResult = program.run(ImmutableMap.of("intArg", ImmutableList.of(99, 88)));
         Object intArg = ((Record) ((List<Object>)myResult.getResult("program_arguments").get().getResult()).get(0)).get("intArg");
         Assert.assertEquals(intArg, ImmutableList.of(99, 88));
         Object doubleArg = ((Record) ((List<Object>)myResult.getResult("program_arguments").get().getResult()).get(0)).get("doubleArg");
         Assert.assertEquals(doubleArg, -0.1);
    } 
    
    private void dumpDebugInfo(CompiledProgram program, ProgramResult myResult) throws InterruptedException, ExecutionException, IOException {
        if (DEBUG_DUMP) {
            //program.dump(System.err);
//            TraceRequest trace = myResult.getEnd().get();
//            TraceFormatter.dump(System.err, trace);
        }
    }
}
