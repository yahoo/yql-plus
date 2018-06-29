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
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class PrimitiveComparisonTest {


    @Test
    public void testInt32() throws Exception {
        PrimitiveRecord record = new PrimitiveRecord('a', (byte) 1, (short) 2, 2, 4L, true, 1.0f, 2.0);
        PrimitiveRecord other = new PrimitiveRecord('b', (byte) 4, (short) 3, 3, 1L, true, 2.0f, 1.0);
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new PrimitiveSource(ImmutableList.of(record, other))));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@arg_int int32, @arr_int array<int32>);" +
                "SELECT * FROM source WHERE p_int = 2        OUTPUT AS f1;" +
                "SELECT * FROM source WHERE b_int = 2        OUTPUT AS f2;" +
                "SELECT * FROM source WHERE 2 = p_int        OUTPUT AS f3;" +
                "SELECT * FROM source WHERE 2 = b_int        OUTPUT AS f4;" +
                "SELECT * FROM source WHERE @arg_int = p_int OUTPUT AS f5;" +
                "SELECT * FROM source WHERE @arg_int = b_int OUTPUT AS f6;" +
                "SELECT * FROM source WHERE p_int = @arg_int OUTPUT AS f7;" +
                "SELECT * FROM source WHERE b_int = @arg_int OUTPUT AS f8;" +
                "SELECT * FROM source WHERE p_int IN (2)     OUTPUT AS f9;" +
                "SELECT * FROM source WHERE b_int IN (2)     OUTPUT AS f10;" +
                "SELECT * FROM source WHERE p_int IN (@arg_int)     OUTPUT AS f11;" +
                "SELECT * FROM source WHERE b_int IN (@arg_int)     OUTPUT AS f12;" +
                "SELECT * FROM source WHERE p_int IN (@arr_int)     OUTPUT AS f13;" +
                "SELECT * FROM source WHERE b_int IN (@arr_int)     OUTPUT AS f14;" +

                "SELECT * FROM source WHERE p_int != 2        OUTPUT AS n1;" +
                "SELECT * FROM source WHERE b_int != 2        OUTPUT AS n2;" +
                "SELECT * FROM source WHERE 2 != p_int        OUTPUT AS n3;" +
                "SELECT * FROM source WHERE 2 != b_int        OUTPUT AS n4;" +
                "SELECT * FROM source WHERE @arg_int != b_int OUTPUT AS n5;" +
                "SELECT * FROM source WHERE b_int != @arg_int OUTPUT AS n6;" +
                "SELECT * FROM source WHERE b_int != @arg_int OUTPUT AS n7;" +
                "SELECT * FROM source WHERE p_int NOT IN (2)     OUTPUT AS n8;" +
                "SELECT * FROM source WHERE b_int NOT IN (2)     OUTPUT AS n9;" +
                "SELECT * FROM source WHERE p_int NOT IN (@arg_int)     OUTPUT AS n10;" +
                "SELECT * FROM source WHERE b_int NOT IN (@arg_int)     OUTPUT AS n11;" +
                "SELECT * FROM source WHERE b_int NOT IN (@arr_int)     OUTPUT AS n12;" +
                "SELECT * FROM source WHERE p_int NOT IN (@arr_int)     OUTPUT AS n13;"
        );
        ProgramResult rez = program.run(ImmutableMap.of("arg_int", 2, "arr_int", ImmutableList.of(2)), true);
        for (int i = 1; i < 15; ++i) {
            Assert.assertEquals(rez.getResult("f" + i).get().getResult(), ImmutableList.of(record), "result f" + i);
        }
        for (int i = 1; i < 14; ++i) {
            Assert.assertEquals(rez.getResult("n" + i).get().getResult(), ImmutableList.of(other), "result n" + i);
        }
        // TODO: test widening conversions
        // TODO: split into many programs, probably
    }
}
