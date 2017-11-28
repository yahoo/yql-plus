package com.yahoo.yqlplus.engine.java;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;
import com.google.inject.util.Providers;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.engine.internal.bytecode.CompilingTestBase;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.ModuleType;
import com.yahoo.yqlplus.engine.internal.source.ExportUnitGenerator;
import com.yahoo.yqlplus.language.parser.Location;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class EvaluateStatementTest extends CompilingTestBase {

    public static class TestModule implements Exports {
        @Export
        public List<String> names() {
            return ImmutableList.of("a", "b", "c");
        }

        @Export
        public ListenableFuture<List<String>> namesAsync() {
            return Futures.immediateFuture(names());
        }

        @Export
        public CompletableFuture<List<String>> namesJavaAsync() {
            return CompletableFuture.completedFuture(names());
        }
    }

    @Override
    public ModuleType findModule(Location location, ContextPlanner planner, List<String> modulePath) {
        String name = Joiner.on(".").join(modulePath);
        if("test".equals(name)) {
            Provider<Exports> moduleProvider = Providers.of(new TestModule());
            ExportUnitGenerator adapter = new ExportUnitGenerator(planner.getGambitScope());
            return adapter.apply(modulePath, moduleProvider);
        }
        return super.findModule(location, planner, modulePath);
    }

    @Test
    public void requireEvaluateCallSync() throws Exception {
        List<String> result = runQueryProgram("EVALUATE test.names()");
        Assert.assertEquals(result, ImmutableList.of("a", "b", "c"));
    }

    @Test
    public void requireEvaluateCallASyncGuava() throws Exception {
        List<String> result = runQueryProgram("EVALUATE test.namesAsync()");
        Assert.assertEquals(result, ImmutableList.of("a", "b", "c"));
    }


    @Test
    public void requireEvaluateCallASyncJava() throws Exception {
        List<String> result = runQueryProgram("EVALUATE test.namesJavaAsync()");
        Assert.assertEquals(result, ImmutableList.of("a", "b", "c"));
    }
}
