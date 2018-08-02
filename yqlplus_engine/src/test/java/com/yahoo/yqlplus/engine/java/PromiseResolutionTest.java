/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.sources.KeyedSource;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public class PromiseResolutionTest extends ProgramTestBase {
    public static class IntegerKeyed {
        public int woeid;

        public IntegerKeyed(int woeid) {
            this.woeid = woeid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntegerKeyed that = (IntegerKeyed) o;

            return woeid == that.woeid;
        }

        @Override
        public int hashCode() {
            return woeid;
        }
    }

    public static class FutureSource implements Source {
        @Query
        public Future<List<IntegerKeyed>> lookup(@Key("woeid") final Integer integer) {
            return ForkJoinPool.commonPool().submit(new Callable<List<IntegerKeyed>>() {
                @Override
                public List<IntegerKeyed> call() {
                    return ImmutableList.of(new IntegerKeyed(integer));
                }
            });
        }
    }

    public static class CompletableFutureSource implements Source {
        @Query
        public CompletableFuture<List<IntegerKeyed>> lookup(@Key("woeid") final Integer integer) {
            final CompletableFuture<List<IntegerKeyed>> result = new CompletableFuture<>();
            ForkJoinPool.commonPool().submit(new Runnable() {
                @Override
                public void run() {
                    result.complete(ImmutableList.of(new IntegerKeyed(integer)));
                }
            });
            return result;
        }
    }

    @Test
    public void requireFutureResolution() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", new FutureSource());
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE woeid = 10 OUTPUT AS f1;\n");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.of());
        List<KeyedSource.IntegerKeyed> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1, ImmutableList.of(new IntegerKeyed(10)));
    }

    @Test
    public void requireCompletableFutureResolution() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", new CompletableFutureSource());
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE woeid = 10 OUTPUT AS f1;\n");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.of());
        List<KeyedSource.IntegerKeyed> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1, ImmutableList.of(new IntegerKeyed(10)));
    }
}
