package com.yahoo.yqlplus.engine.java;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;
import com.google.inject.util.Providers;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.bytecode.CompilingTestBase;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.ModuleType;
import com.yahoo.yqlplus.engine.internal.source.ExportUnitGenerator;
import com.yahoo.yqlplus.language.parser.Location;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class EvaluateStatementTest extends CompilingTestBase {

    public static class TestModule implements Exports {
        @Export
        public List<String> names() {
            return ImmutableList.of("a", "b", "c");
        }

        @Export
        public List<String> names_by_key(List<String> keys) {
            return keys;
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

    public static class TestRecord implements Record {
        private Map<String,Object> data;

        public TestRecord(Map<String, Object> data) {
            this.data = data;
        }


        public TestRecord(String id) {
            this(ImmutableMap.of("id", id));
        }

        @Override
        public Iterable<String> getFieldNames() {
            return data.keySet();
        }

        @Override
        public Object get(String field) {
            return data.get(field);
        }
    }

    public static class TestSource implements Source {
        @Query
        public List<TestRecord> scan() {
            return ImmutableList.of();
        }

        @Query
        public List<TestRecord> oneArg(Object any) {
            return ImmutableList.of();
        }

        @Query
        public List<TestRecord> twoArg(Object any, Object any2) {
            return ImmutableList.of();
        }

        @Query
        public List<TestRecord> threeArg(Object any, Object any2, Object three) {
            return ImmutableList.of();
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

    @Test
    public void requireComplexPlanningExample() throws Exception {
        defineSource("table4", TestSource.class);
        defineSource("table5", TestSource.class);
        defineSource("table10", TestSource.class);
        CompiledProgram program = compileProgramResource("dashboard.yql");
        List<String> output = Lists.newArrayList(program.run(ImmutableMap.of(), true).getResultNames());
        Assert.assertEquals(output.size(), 8);
    }
}
