/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.MetricType;
import com.yahoo.cloud.metrics.api.RequestEvent;
import com.yahoo.cloud.metrics.api.RequestMetric;
import com.yahoo.cloud.metrics.api.StandardRequestEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.ExecuteScoped;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.guice.SeededKeyProvider;
import com.yahoo.yqlplus.api.trace.TraceEntry;
import com.yahoo.yqlplus.api.trace.TraceRequest;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.CompiledProgram.ArgumentInfo;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.DependencyNotFoundException;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.java.JavaTestModule.MetricModule;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;
import com.yahoo.yqlplus.engine.scope.MapExecutionScope;
import com.yahoo.yqlplus.engine.sources.ArraySyntaxTestSource;
import com.yahoo.yqlplus.engine.sources.ArraySyntaxTestSource.ArraySyntaxTestRecord;
import com.yahoo.yqlplus.engine.sources.AsyncInsertMovieSource;
import com.yahoo.yqlplus.engine.sources.AsyncUpdateMovieSource;
import com.yahoo.yqlplus.engine.sources.BaseUrlMapSource;
import com.yahoo.yqlplus.engine.sources.BatchKeySource;
import com.yahoo.yqlplus.engine.sources.BoxedParameterSource;
import com.yahoo.yqlplus.engine.sources.BulkResponse;
import com.yahoo.yqlplus.engine.sources.CollectionFunctionsUdf;
import com.yahoo.yqlplus.engine.sources.ErrorSource;
import com.yahoo.yqlplus.engine.sources.ExecuteScopedInjectedSource;
import com.yahoo.yqlplus.engine.sources.FRSource;
import com.yahoo.yqlplus.engine.sources.InjectedArgumentSource;
import com.yahoo.yqlplus.engine.sources.InsertMovieSourceSingleField;
import com.yahoo.yqlplus.engine.sources.InsertSourceMissingSetAnnotation;
import com.yahoo.yqlplus.engine.sources.InsertSourceWithDuplicateSetParameters;
import com.yahoo.yqlplus.engine.sources.InsertSourceWithMultipleInsertMethods;
import com.yahoo.yqlplus.engine.sources.IntSource;
import com.yahoo.yqlplus.engine.sources.JsonArraySource;
import com.yahoo.yqlplus.engine.sources.KeyTypeSources.KeyTypeSource1;
import com.yahoo.yqlplus.engine.sources.KeyTypeSources.KeyTypeSource2;
import com.yahoo.yqlplus.engine.sources.KeyTypeSources.KeyTypeSource3;
import com.yahoo.yqlplus.engine.sources.KeyTypeSources.KeyTypeSource4;
import com.yahoo.yqlplus.engine.sources.KeyedSource;
import com.yahoo.yqlplus.engine.sources.KeyedSource.IntegerKeyed;
import com.yahoo.yqlplus.engine.sources.ListOfMapSource;
import com.yahoo.yqlplus.engine.sources.LongDoubleMovieSource;
import com.yahoo.yqlplus.engine.sources.LongMovie;
import com.yahoo.yqlplus.engine.sources.LongSource;
import com.yahoo.yqlplus.engine.sources.MapSource;
import com.yahoo.yqlplus.engine.sources.MapSource.SampleId;
import com.yahoo.yqlplus.engine.sources.MapSyntaxTestSource;
import com.yahoo.yqlplus.engine.sources.MapSyntaxTestSource.MapSyntaxTestRecord;
import com.yahoo.yqlplus.engine.sources.MethodTracingSource;
import com.yahoo.yqlplus.engine.sources.MetricEmitterSource;
import com.yahoo.yqlplus.engine.sources.Movie;
import com.yahoo.yqlplus.engine.sources.MovieSource;
import com.yahoo.yqlplus.engine.sources.MovieSourceDefaultValueWithoutSet;
import com.yahoo.yqlplus.engine.sources.MovieSourceWithLongUuid;
import com.yahoo.yqlplus.engine.sources.MovieSourceWithMetricEmitter;
import com.yahoo.yqlplus.engine.sources.MovieUDF;
import com.yahoo.yqlplus.engine.sources.NestedMapSource;
import com.yahoo.yqlplus.engine.sources.NestedSource;
import com.yahoo.yqlplus.engine.sources.PersonMakerSource;
import com.yahoo.yqlplus.engine.sources.Sample;
import com.yahoo.yqlplus.engine.sources.SampleListSource;
import com.yahoo.yqlplus.engine.sources.SampleListSourceWithBoxedParams;
import com.yahoo.yqlplus.engine.sources.SampleListSourceWithUnboxedParams;
import com.yahoo.yqlplus.engine.sources.SampleResultSource;
import com.yahoo.yqlplus.engine.sources.GenericFieldResultSource;
import com.yahoo.yqlplus.engine.sources.SingleIntegerKeySource;
import com.yahoo.yqlplus.engine.sources.SingleIntegerKeySourceWithSkipEmptyOrZeroSetToTrue;
import com.yahoo.yqlplus.engine.sources.SingleKeySource;
import com.yahoo.yqlplus.engine.sources.SingleListOfStringKeySourceWithSkipEmptyOrZeroSetToTrue;
import com.yahoo.yqlplus.engine.sources.SingleStringKeySourceWithSkipEmptyOrZeroSetToTrue;
import com.yahoo.yqlplus.engine.sources.StatusSource;
import com.yahoo.yqlplus.engine.sources.StringUtilUDF;
import com.yahoo.yqlplus.engine.sources.UnrulyRequestSource;
import com.yahoo.yqlplus.engine.sources.UnrulyRequestSource.UnrulyRequestHandle;
import com.yahoo.yqlplus.engine.sources.UnrulyRequestSource.UnrulyRequestRecord;
import com.yahoo.yqlplus.engine.sources.UpdateMovieSource;
import com.yahoo.yqlplus.engine.sources.UpdateMovieSourceWithUnsortedParameters;
import com.yahoo.yqlplus.engine.tools.TraceFormatter;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

@Test()
public class JavaProgramCompilerTest {
    private static final boolean DEBUG_DUMP = false;
    
    @Test
    public void testGenericResult() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("genericResult", SampleResultSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT result.id FROM genericResult OUTPUT AS sample;");
        List<Record> ids = program.run(ImmutableMap.<String, Object>of(), true).getResult("sample").get().getResult();
        assertEquals("id", ids.get(0).get("id"));
        assertEquals("id", ids.get(1).get("id"));
    }

    @Test
    public void testGenericFieldResult() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("genericResult", GenericFieldResultSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT myField.id FROM genericResult OUTPUT AS sample;");
        List<Record> ids = program.run(ImmutableMap.<String, Object>of(), true).getResult("sample").get().getResult();
        Assert.assertEquals("id", ids.get(0).get("id"));
    }
    
    @Test
    public void testExplore() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("CREATE TEMPORARY TABLE frobaz AS (SELECT * FROM people WHERE id IN ('1', '2'));" +
                "SELECT * FROM frobaz OUTPUT AS foo;" +
                "SELECT * FROM people WHERE score = 1 OUTPUT AS score1;" +
                "SELECT * FROM people WHERE id IN (SELECT id FROM people) OUTPUT AS baz;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 2);
        Assert.assertEquals(foo.get(0).getId(), "1");
        Assert.assertEquals(foo.get(1).getId(), "2");
        //TraceRequest trace = myResult.getEnd().get();
        //TraceFormatter.dump(System.err, trace);
        YQLResultSet rez2 = myResult.getResult("score1").get();
        List<Person> result = rez2.getResult();
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0).getId(), "2");
        Assert.assertEquals(result.get(0).getValue(), "joe");
        Assert.assertEquals(result.get(0).getScore(), 1);
        YQLResultSet rez3 = myResult.getResult("baz").get();
        List<Person> result3 = rez3.getResult();
        Assert.assertEquals(result3.size(), 3);
    }

    @Test
    public void testJoinTempTable() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("CREATE TEMPORARY TABLE frobaz AS (SELECT * FROM people WHERE id IN ('1', '2'));" +
                "CREATE TEMPORARY TABLE frobaz2 AS (SELECT * FROM people WHERE id IN ('1', '2'));" +
                "SELECT * FROM frobaz JOIN frobaz2 on frobaz.id = frobaz2.id OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertTrue(((Person)foo.get(0).get("frobaz")).getId().equals("1"));
        Assert.assertTrue(((Person)foo.get(0).get("frobaz2")).getId().equals("1"));
        Assert.assertTrue(((Person)foo.get(1).get("frobaz")).getId().equals("2"));
        Assert.assertTrue(((Person)foo.get(1).get("frobaz2")).getId().equals("2"));
    }

    @Test
    public void testB1Only() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM innersource WHERE id = '1' OUTPUT AS b1;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet b1 = myResult.getResult("b1").get();
        List<Person> b1r = b1.getResult();
        Assert.assertEquals(b1r.size(), 1);
        Assert.assertEquals(b1r.get(0).getId(), "1");
        dumpDebugInfo(program, myResult);
    }

    public static void dumpDebugInfo(CompiledProgram program, ProgramResult myResult) throws InterruptedException, ExecutionException, IOException {
        if (DEBUG_DUMP) {
            //program.dump(System.err);
            TraceRequest trace = myResult.getEnd().get();
            TraceFormatter.dump(System.err, trace);
        }
    }

    @Test
    public void testTrace() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM trace OUTPUT AS b1;");
        TraceRequest trace = program.run(ImmutableMap.<String, Object>of(), true).getEnd().get();
        for (TraceEntry entry : trace.getEntries()) {
            if ("MINE".equals(entry.getName())) {
                return; // found it
            }
        }
        Assert.fail("Did not find our trace entry");
    }

    @Test
    public void testMethodTrace() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("traceMethod", MethodTracingSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("testMethodTrace", "SELECT * FROM traceMethod OUTPUT AS b1;");
        TraceRequest trace = program.run(ImmutableMap.<String, Object>of(), true).getEnd().get();
        for (TraceEntry entry : trace.getEntries()) {
            if (entry.getGroup().equals("program")) {
                assertEquals("testMethodTrace", entry.getName());
            }
            if (entry.getGroup().equals("query")) {
              assertEquals("b1", entry.getName());
            }
            if (entry.getGroup().equals("method")) {
                assertEquals("scan", entry.getName());
                assertTrue((entry.getEndMilliseconds() - entry.getStartMilliseconds()) > 5);
            }
            if ("MINE".equals(entry.getName())) {
                return; // found it
            }
        }
        Assert.fail("Did not find our trace entry");
    }

    @Test
    public void testMetricEmitter() throws Exception {
        MetricModule metricModule = new MetricModule(new MetricDimension().with("key1", "value1").with("key2", "value2"), true);
        JavaTestModule javaTestModule = new JavaTestModule(metricModule);
        Injector injector = Guice.createInjector(javaTestModule, new SourceBindingModule("emitter", MetricEmitterSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM emitter OUTPUT AS b1;");
        StandardRequestEmitter requestEmitter =  metricModule.getStandardRequestEmitter();
        ExecutionScope scope = new MapExecutionScope()
            .bind(TaskMetricEmitter.class, requestEmitter.start("program", "<string>"));

        program.run(ImmutableMap.<String, Object>of(), true, scope).getEnd().get();
        requestEmitter.complete();
        RequestEvent requestEvent = javaTestModule.getRequestEvent();
        Queue<RequestMetric> metrics = requestEvent.getMetrics();
        boolean foundTask = false;
        boolean foundLatency = false;
        for (RequestMetric metric : metrics) {
            if (",subtask=createResponse,method=getPerson,source=emitter,query=b1,program=<string>,key2=value2,key1=value1".equals(dimensionStr(metric.getMetric().getDimension())))
                foundTask = true;
            if (metric.getMetric().getType() == MetricType.DURATION && metric.getMetric().getName().equals("requestLatency"))
                foundLatency = true;
        }
        Assert.assertTrue(foundTask, "found metric task");
        Assert.assertTrue(foundLatency, "found metric latency");
    }

    @Test
    public void testMetricEmitterGivenName() throws Exception {
        MetricModule metricModule = new MetricModule(new MetricDimension().with("key1", "value1").with("key2", "value2"), "emitterTestProgram", true);
        JavaTestModule javaTestModule = new JavaTestModule(metricModule);
        Injector injector = Guice.createInjector(javaTestModule, new SourceBindingModule("emitter", MetricEmitterSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("emitterTestProgram", "SELECT * FROM emitter OUTPUT AS b1;");
        StandardRequestEmitter requestEmitter =  metricModule.getStandardRequestEmitter();
        ExecutionScope scope = new MapExecutionScope()
            .bind(TaskMetricEmitter.class, requestEmitter.start("program", "emitterTestProgram"));
        program.run(ImmutableMap.<String, Object>of(), true, scope).getEnd().get();
        RequestEvent requestEvent = javaTestModule.getRequestEvent();
        Queue<RequestMetric> metrics = requestEvent.getMetrics();
        boolean foundTask = false;
        boolean foundLatency = false;
        for (RequestMetric metric : metrics) {
            if (",subtask=createResponse,method=getPerson,source=emitter,query=b1,program=emitterTestProgram,key2=value2,key1=value1".equals(dimensionStr(metric.getMetric().getDimension())))
                foundTask = true;
            if (metric.getMetric().getType() == MetricType.DURATION && metric.getMetric().getName().equals("requestLatency"))
                foundLatency = true;
        }
        Assert.assertTrue(foundTask && foundLatency);
    }

    private String dimensionStr(MetricDimension dimension) {
        StringBuilder sb = new StringBuilder();
        if (dimension.getKey() != null) {
            sb.append(",")
                    .append(dimension.getKey())
                    .append("=")
                    .append(dimension.getValue());
            while (dimension.getParent() != null) {
                dimension = dimension.getParent();
                if (dimension.getKey() != null) {
                    sb.append(",")
                            .append(dimension.getKey())
                            .append("=")
                            .append(dimension.getValue());
                } else {
                    break;
                }
            }
        }
        return sb.toString();
    }

    @Test
    public void testInnerAsync() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from asyncsource OUTPUT as foo;" +
                "SELECT * FROM asyncsource WHERE id = '1' OUTPUT AS b1;" +
                "SELECT * FROM asyncsource WHERE id = '2' OUTPUT as b2;" +
                "SELECT * FROM asyncsource WHERE id IN ('1', '2') OUTPUT as b3;" +
                "SELECT * FROM asyncsource WHERE id IN (SELECT id FROM innersource) OR id = '3' ORDER BY id DESC OUTPUT as b4;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Assert.assertEquals(foo.get(0).getId(), "1");

        YQLResultSet b1 = myResult.getResult("b1").get();
        List<Person> b1r = b1.getResult();
        Assert.assertEquals(b1r.size(), 1);
        Assert.assertEquals(b1r.get(0).getId(), "1");

        YQLResultSet b2 = myResult.getResult("b2").get();
        List<Person> b2r = b2.getResult();
        Assert.assertEquals(b2r.size(), 0);

        Assert.assertEquals(myResult.getResult("b3").get().getResult(), Lists.newArrayList(new Person("1", "joe", 1)));
        Assert.assertEquals(myResult.getResult("b4").get().getResult(), Lists.newArrayList(new Person("3", "smith", 1), new Person("1", "joe", 1)));

        dumpDebugInfo(program, myResult);
    }

    public static class UnrulyRequestModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(ExecutionScope.class).annotatedWith(Names.named("compile")).toInstance(
                    new MapExecutionScope().bind(UnrulyRequestHandle.class, new UnrulyRequestHandle(-1))
            );
            MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
            sourceBindings.addBinding("unruly").to(UnrulyRequestSource.class);

            bind(UnrulyRequestHandle.class)
                    .toProvider(SeededKeyProvider.<UnrulyRequestHandle>seededKeyProvider())
                    .in(ExecuteScoped.class);
        }
    }

    private static ExecutionScope createUnrulyScope(int id) {
        return new MapExecutionScope()
                .bind(UnrulyRequestHandle.class, new UnrulyRequestHandle(id));
    }

    @Test
    public void testExecuteScoped() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new UnrulyRequestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from unruly OUTPUT as foo;");
        for (int i = 0; i < 20; ++i) {
            ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true, createUnrulyScope(i));
            Assert.assertEquals(myResult.getResult("foo").get().getResult(), ImmutableList.of(new UnrulyRequestRecord(i)));
        }
    }

    /*
     * Have ProgramResult expose all the ExecuteScoped objects that were created during the program execution
     *
     * This test queries a program source which is injected with an ExecuteScoped object and returns that as its query result.
     * The test then asserts that the returned instance is contained in the collection of ExecuteScoped objects available from
     * the ProgramResult.
     */
    @Test
    public void testExecuteScopedObjectsExposedInProgramResult() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new ExecuteScopedProviderModule(),
                new SourceBindingModule("executeScopedInjectedSource", ExecuteScopedInjectedSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from executeScopedInjectedSource OUTPUT as foo;");
        ProgramResult programResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> list = programResult.getResult("foo").get().getResult();
        // Get the Foo instance that is returned in the program result
        ExecuteScopedProviderModule.Foo fooResult = (ExecuteScopedProviderModule.Foo) list.get(0);
        // Get the Foo instance contained in the map of ExecuteScoped objects exposed by the program result
        Assert.assertTrue(programResult.getExecuteScopedObjects().contains(fooResult),
                "Collection of ExecuteScoped objects exposed by ProgramResult does not contain Foo instance");
    }

    @Test
    public void testInjectedParameters() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new UnrulyRequestModule(), new SourceBindingModule("iasource", InjectedArgumentSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from iasource(10) OUTPUT as foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true, createUnrulyScope(100));
        Assert.assertEquals(myResult.getResult("foo").get().getResult(), ImmutableList.of(new UnrulyRequestRecord(10)));

        program = compiler.compile("SELECT * from iasource OUTPUT as foo;");
        myResult = program.run(ImmutableMap.<String, Object>of(), true, createUnrulyScope(100));
        Assert.assertEquals(myResult.getResult("foo").get().getResult(), ImmutableList.of(new UnrulyRequestRecord(100)));
    }

    @Test
    public void testBatchKey() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from source WHERE id = '1' OUTPUT as f1;" +
                "SELECT * FROM source WHERE id = '1' OR id = '2' OUTPUT AS f2;" +
                "SELECT * FROM source WHERE id IN ('1', '2', '3') OUTPUT AS f3;" +
                "SELECT * FROM source(10) WHERE id IN ('1', '2', '3') OUTPUT AS f4;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
        Assert.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2)));
        Assert.assertEquals(rez.getResult("f3").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2), new Person("3", "3", 3)));
        Assert.assertEquals(rez.getResult("f4").get().getResult(), ImmutableList.of(new Person("1", "1", 10), new Person("2", "2", 10), new Person("3", "3", 10)));
    }

    @Test
    public void testBatchKeyOrClauses() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id = '1' OR id = '2' OUTPUT AS f2;");
//        program.dump(System.err);
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2)));
    }

    @Test
    public void requireConstantLimit() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@lmt int32, @off int32);" +
                "CREATE VIEW foo AS SELECT * FROM source WHERE id IN ('1', '2', '3');" +
                "SELECT * FROM foo LIMIT 1                OUTPUT AS f1;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("lmt", 1, "off", 1), true);
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
    }

    @Test
    public void requireConstantSlice() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@lmt int32, @off int32);" +
                "CREATE VIEW foo AS SELECT * FROM source WHERE id IN ('1', '2', '3');" +
                "SELECT * FROM foo LIMIT 1    OFFSET 1    OUTPUT AS f2;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("lmt", 1, "off", 1), true);
        Assert.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("2", "2", 2)));
    }


    @Test
    public void requireArgumentSlice() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@lmt int32, @off int32);" +
                "CREATE VIEW foo AS SELECT * FROM source WHERE id IN ('1', '2', '3');" +
                "SELECT * FROM foo LIMIT @lmt OFFSET @off OUTPUT AS f3;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("lmt", 1, "off", 1), true);
        Assert.assertEquals(rez.getResult("f3").get().getResult(), ImmutableList.of(new Person("2", "2", 2)));
    }

    @Test
    public void requireArgumentLimit() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@lmt int32, @off int32);" +
                "CREATE VIEW foo AS SELECT * FROM source WHERE id IN ('1', '2', '3');" +
                "SELECT * FROM foo LIMIT @lmt             OUTPUT AS f4;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("lmt", 1, "off", 1), true);
        Assert.assertEquals(rez.getResult("f4").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
    }


    @Test
    public void requireArgumentOffset() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@lmt int32, @off int32);" +
                "CREATE VIEW foo AS SELECT * FROM source WHERE id IN ('1', '2', '3');" +
                "SELECT * FROM foo OFFSET @off            OUTPUT AS f5;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("lmt", 1, "off", 1), true);
        Assert.assertEquals(rez.getResult("f5").get().getResult(), ImmutableList.of(new Person("2", "2", 2), new Person("3", "3", 3)));
    }

    @Test
    public void testSingleKey() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from source WHERE id = '1' OUTPUT as f1;" +
                "SELECT * FROM source WHERE id = '1' OR id = '2' OUTPUT AS f2;" +
                "SELECT * FROM source WHERE id IN ('1', '2', '3') OUTPUT AS f3;" +
                "SELECT * FROM source(10) WHERE id IN ('1', '2', '3') OUTPUT AS f4;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
        Assert.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2)));
        Assert.assertEquals(rez.getResult("f3").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2), new Person("3", "3", 3)));
        Assert.assertEquals(rez.getResult("f4").get().getResult(), ImmutableList.of(new Person("1", "1", 10), new Person("2", "2", 10), new Person("3", "3", 10)));
    }

    @Test
    public void requireStatementIdentification() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from source WHERE id = '1' OUTPUT as f1;" +
                "SELECT * FROM source WHERE id = '1' OR id = '2' OUTPUT AS f2;" +
                "SELECT * FROM source WHERE id IN ('1', '2', '3') OUTPUT AS f3;" +
                "SELECT * FROM source(10) WHERE id IN ('1', '2', '3') OUTPUT AS f4;");
        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    @Test
    public void testSingleKey2() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * from source WHERE id = '1' OUTPUT as f1;" +
                "SELECT * FROM source WHERE id = '1' OR id = '2' OUTPUT AS f2;" +
                "SELECT * FROM source(10) WHERE id IN ('1', '2', '3') OUTPUT AS f3;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("f1").get().getResult(), ImmutableList.of(new Person("1", "1", 1)));
        Assert.assertEquals(rez.getResult("f2").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 2)));
        Assert.assertEquals(rez.getResult("f3").get().getResult(), ImmutableList.of(new Person("1", "1", 10), new Person("2", "2", 10), new Person("3", "3", 10)));
        dumpDebugInfo(program, rez);
    }

    @Test
    public void testSimpleMerge() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(
                "SELECT * FROM (SELECT * from source WHERE id = '1' MERGE SELECT * FROM source(10) WHERE id = '2' MERGE SELECT * FROM source WHERE id = '3') ORDER BY id OUTPUT AS f4;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("f4").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("2", "2", 10), new Person("3", "3", 3)));
    }

    @Test(expectedExceptions = {YQLTypeException.class}, expectedExceptionsMessageRegExp = "\\Q@Query method error: com.yahoo.yqlplus.engine.sources.KeyTypeSources$KeyTypeSource4.getRecord: Property @Key('fancypants') for method com.yahoo.yqlplus.engine.sources.KeyTypeSources$KeyTypeSource4.getRecord does not exist on return type com.yahoo.yqlplus.engine.sources.KeyTypeSources$KeyTypeRecord\\E")
    public void testKeyPropertyExistsValidation() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", KeyTypeSource4.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id = 1;");
    }

    @Test(expectedExceptions = {YQLTypeException.class}, expectedExceptionsMessageRegExp = "\\Q@Query method error: com.yahoo.yqlplus.engine.sources.KeyTypeSources$KeyTypeSource1.getRecord: class com.yahoo.yqlplus.engine.sources.KeyTypeSources$KeyTypeRecord property id is java.lang.String while @Key('id') type is java.lang.Integer: @Key('id') argument type java.lang.Integer cannot be coerced to property 'id' type java.lang.String in method com.yahoo.yqlplus.engine.sources.KeyTypeSources$KeyTypeSource1.getRecord")
    public void testKeyPropertyTypesMatchValidation() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", KeyTypeSource1.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id = 1;");
    }

    @Test
    public void requirePrimitiveIntegerKey() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", KeyTypeSource2.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT id FROM source WHERE value = 1 OUTPUT AS f1;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(((List<Record>) myResult.getResult("f1").get().getResult()).get(0).get("id"), "1");
    }

    @Test
    public void testKeyUnboxingValidation() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", KeyTypeSource3.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("SELECT * FROM source WHERE value2 = 1;");
    }

    @Test
    public void testArraySyntax() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", ArraySyntaxTestSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people[0] person FROM source OUTPUT AS f1;\n" +
                "SELECT dudes[0] dude FROM source OUTPUT AS f2;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 1);
        Assert.assertEquals(f1.get(0).get("person"), new ArraySyntaxTestRecord().people.get(0));
        List<Record> f2 = myResult.getResult("f2").get().getResult();
        Assert.assertEquals(f2.size(), 1);
        Assert.assertEquals(f2.get(0).get("dude"), new ArraySyntaxTestRecord().getDudes().get(0));
    }

    @Test
    public void testNestedArraySyntax() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", NestedSource.class, "lookup", PersonMakerSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT result.people[0] person FROM source OUTPUT AS f1;\n" +
                "SELECT result.dudes[0] dude FROM source OUTPUT AS f2;" +
                "SELECT * FROM lookup WHERE id IN (SELECT result.people[0].id FROM source) OUTPUT AS f3;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 1);
        Assert.assertEquals(f1.get(0).get("person"), new ArraySyntaxTestRecord().people.get(0));
        List<Record> f2 = myResult.getResult("f2").get().getResult();
        Assert.assertEquals(f2.size(), 1);
        Assert.assertEquals(f2.get(0).get("dude"), new ArraySyntaxTestRecord().getDudes().get(0));
        List<Person> f3 = myResult.getResult("f3").get().getResult();
        Assert.assertEquals(f3, ImmutableList.of(new Person("2", "2", 2)));

    }

    @Test
    public void testMissingSpace() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@strArg1 array<string>= ['array test1'], @strArg2 array<string> = ['array test2']);" +
                "SELECT @strArg1, @strArg2 OUTPUT AS program_arguments;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("program_arguments").get().getResult();
        Assert.assertEquals(f1.get(0).get("strArg1").toString(), "[array test1]");
        Assert.assertEquals(f1.get(0).get("strArg2").toString(), "[array test2]");
    }

    @Test
    public void testMapSyntax() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", MapSyntaxTestSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people['joe'] person FROM source OUTPUT AS f1;\n" +
                "SELECT dudes['dude'] dude FROM source OUTPUT AS f2;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 1);
        Assert.assertEquals(f1.get(0).get("person"), new MapSyntaxTestRecord().people.get("joe"));
        List<Record> f2 = myResult.getResult("f2").get().getResult();
        Assert.assertEquals(f2.size(), 1);
        Assert.assertEquals(f2.get(0).get("dude"), new MapSyntaxTestRecord().getDudes().get("dude"));
    }

    @Test
    public void testNestedMapSyntax() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", NestedMapSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT result.people['joe'] person FROM source OUTPUT AS f1;\n" +
                "SELECT result.dudes['dude'] dude FROM source OUTPUT AS f2;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 1);
        Assert.assertEquals(f1.get(0).get("person"), new MapSyntaxTestRecord().people.get("joe"));
        List<Record> f2 = myResult.getResult("f2").get().getResult();
        Assert.assertEquals(f2.size(), 1);
        Assert.assertEquals(f2.get(0).get("dude"), new MapSyntaxTestRecord().getDudes().get("dude"));
    }

    @Test
    public void testIntegerKeys() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE woeid = 10 OUTPUT AS f1;\n");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<IntegerKeyed> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1, ImmutableList.of(new IntegerKeyed(10)));
    }
    
    @Test
    public void testViewWithParameters() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", BatchKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@id1 string, @id2 string);" +
                "CREATE VIEW foo AS SELECT * FROM source WHERE id IN (@id1 , @id2);" +
                "SELECT * FROM foo OUTPUT AS output;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("id1", "2", "id2", "3"), true);
        Assert.assertEquals(rez.getResult("output").get().getResult(), ImmutableList.of(new Person("2", "2", 2), new Person("3", "3", 3)));
    }

    @Test
    public void testIntegerKeysArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@woeid int32);\n" +
                "SELECT * FROM source WHERE woeid = @woeid OUTPUT AS f1;\n");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of("woeid", 10), true);
        List<IntegerKeyed> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1, ImmutableList.of(new IntegerKeyed(10)));
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testIntegerKeysArgumentMissing() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@woeid int32);\n" +
                "SELECT * FROM source WHERE woeid = @woeid OUTPUT AS f1;\n");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<IntegerKeyed> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1, ImmutableList.of(new IntegerKeyed(10)));
    }

    @Test
    public void testIntegerKeysArgumentDefault() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@woeid int32 = 1234);\n" +
                "SELECT * FROM source WHERE woeid = @woeid OUTPUT AS f1;\n");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of("woeid", 10), true);
        List<IntegerKeyed> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1, ImmutableList.of(new IntegerKeyed(10)));
    }

    @Test
    public void testLiteralMap() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("box", UnboxedArgument.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT {'foo' : 'bar', 'box' : box.add(1, 1)} f1 OUTPUT AS f1;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.get(0).get("f1"), ImmutableMap.<String, Object>builder().put("foo", "bar").put("box", 2).build());
    }

    @Test
    public void testLiteralMapNulls() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("box", UnboxedArgument.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT {'foo' : 'bar', 'box' : box.add(1, 1), 'nil' : box.nil()} f1 OUTPUT AS f1;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.get(0).get("f1"), ImmutableMap.<String, Object>builder().put("foo", "bar").put("box", 2).build());
    }

    /**
     * Unit test for support empty map
     *
     * @throws Exception
     */
    @Test
    public void testEmptyMap() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT * FROM mapsource({}) WHERE id = 2 OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<MapSource.SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getId(), 2);
    }

    @Test
    public void testMapInputKeyWithDot() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                                                   "SELECT * FROM mapsource({'a.b.c':'abc'}) WHERE id = 2 OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<MapSource.SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getId(), 3);
    }
    
    @Test
    public void testMapInputKeyWithDotExpr() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@count int32 = 15);\n" +
                                                   "SELECT * FROM mapsource({'a.b.c': 'abc', 'a' : @count}) WHERE id = 2 OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<MapSource.SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getId(), 4);
    }
    
    @Test
    public void testMapKeyWithDotAsExpr() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String prgStr = "PROGRAM (@count string = '15');\n" +
                        "SELECT {'a.b.c': 'abc', 'a' : @count};";
        CompiledProgram program = compiler.compile(prgStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List list = myResult.getResult("result1").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Record record = (Record) list.get(0);
        assertEquals("abc", ((Record)record.get("expr")).get("a.b.c"));
        assertEquals("15", ((Record)record.get("expr")).get("a"));
    }
    
    @Test
    public void testMapKeyWithDotAsExprDifferentOrders() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String prgStr = "PROGRAM (@count string = '15');\n" +
                        "SELECT {'a' : @count, 'a.b.c' : 'abc', 'abc' : 'a.b.c', 'cdf' : 'c.d.f', 'c.d.f' : 'cdf'};";
        CompiledProgram program = compiler.compile(prgStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List list = myResult.getResult("result1").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Record record = (Record) list.get(0);
        assertEquals("abc", ((Record)record.get("expr")).get("a.b.c"));
        assertEquals("15", ((Record)record.get("expr")).get("a"));
        assertEquals("a.b.c", ((Record)record.get("expr")).get("abc"));
        assertEquals("c.d.f", ((Record)record.get("expr")).get("cdf"));
        assertEquals("cdf", ((Record)record.get("expr")).get("c.d.f"));
    }
    
    @Test
    public void testConstantyMap() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String prgStr = "PROGRAM ();\n" +
                "    SELECT {'a.b': 'ab', 'a' : 'c'};";
        CompiledProgram program = compiler.compile(prgStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List list = myResult.getResult("result1").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Record record = (Record) list.get(0);
        assertEquals("ab", ((Record)record.get("expr")).get("a.b"));
        assertEquals("c", ((Record)record.get("expr")).get("a"));
    }
    
    @Test
    public void testMapSource() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@mapArgument map<string> = {});\n" +
                "SELECT * FROM mapsource(@mapArgument) WHERE id = 2  OUTPUT AS out;");
        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>builder().put("mapArgument", map).build(), true);
        List<SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(3, list.get(0).getId());
    }

    @Test
    public void testMapSourceWithContantKeyAndArgumentValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@value string = 'value');\n" +
                "SELECT * FROM mapsource({'key':@value}) WHERE id = 2  OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(3, list.get(0).getId());
    }
    
    @Test
    public void testMapSourceWithContantMap() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT * FROM mapsource({'key':'value'}) WHERE id = 2  OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(3, list.get(0).getId());
    }
    
    @Test
    public void testMapArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@mapArgument map<string>);\n" +
                "SELECT mapSize, mapValues FROM mapArgumentSource(@mapArgument) OUTPUT AS out;");
        
        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>builder().put("mapArgument", map).build(), true);
        List<Record> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).get("mapSize"), 1);
        Assert.assertEquals(list.get(0).get("mapValues"), "value");
    }

    @Test
    public void testLiteralArray() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT [1, 2, 3] f1 OUTPUT AS f1;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.get(0).get("f1"), ImmutableList.of(1, 2, 3));
    }

    public static class UnboxedArgument implements Exports {
        @Export
        public int add(int left, int right) {
            return left + right;
        }

        @Export
        public Integer incr(Integer left) {
            return left + 1;
        }

        @Export
        public long now() {
            return 10000L;
        }

        @Export
        public String nil() {
            return null;
        }
    }

    @Test
    public void testUnboxArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("box", UnboxedArgument.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@value int32 = 10);" +
                "SELECT box.add(@value, 1) f1, box.incr(@value) f2, box.incr(1) f3 OUTPUT AS f1;" +
                "SELECT box.now() now OUTPUT AS f2;" +
                "SELECT box.nil() nil OUTPUT AS f3;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals(f1.get(0).get("f1"), 11);
        Assert.assertEquals(f1.get(0).get("f2"), 11);
        Assert.assertEquals(f1.get(0).get("f3"), 2);
        List<Record> f2 = myResult.getResult("f2").get().getResult();
        Assert.assertEquals(f2.get(0).get("now"), 10000L);
        List<Record> f3 = myResult.getResult("f3").get().getResult();
        Assert.assertNull(f3.get(0).get("nil"));
    }

    @Test
    public void testFloatProjection() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", FRSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT floaty, doubley FROM source OUTPUT AS f1;");
        // program.dump(System.err);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("f1").get().getResult();
        Assert.assertEquals((Float) f1.get(0).get("floaty"), 1.0f);
        Assert.assertEquals((Double) f1.get(0).get("doubley"), 2.0);
    }

    @Test
    public void requireThatBooleanCanBeProperlyEvaluated() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@param boolean = false);" +
                "SELECT @param p OUTPUT AS testProgram;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("testProgram").get().getResult();
        Assert.assertEquals(f1.get(0).get("p"), Boolean.FALSE);
    }

    public static class ARecord {
        public BRecord b;

        public ARecord(BRecord b) {
            this.b = b;
        }
    }

    public static class BRecord {
        public String name;

        public BRecord(String name) {
            this.name = name;
        }
    }

    public static class ASource implements Source {
        @Query
        public List<ARecord> scan() {
            return ImmutableList.of(new ARecord(new BRecord("joe")), new ARecord(null));
        }

    }

    @Test
    public void testNullGuardedPropertyResolution() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new ASource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT b.name name FROM source OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> f1 = myResult.getResult("out").get().getResult();
        Assert.assertEquals("joe", f1.get(0).get("name"));
        Assert.assertNull(f1.get(1).get("name"));
    }
    
    @Test
    public void testInsertWithLongDoubleDefault() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new LongDoubleMovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte)0x7a)
                .build(), true);

        List<LongMovie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");
    }

    @Test
    public void testInsert() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);


        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    @Test
    public void testInsertConstant() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values ('1234', @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);
    }

    @Test
    public void testBatchInsertConstant() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values ('1111', @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "('2222', @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "('4444', @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1111", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        insertedMovie = result.get(1);
        Assert.assertEquals("2222", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        insertedMovie = result.get(2);
        Assert.assertEquals("4444", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);
    }

    @Test
    public void testBatchInsert() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @uuid3 string, @title string," +
                "@category string, @prodDate string, @duration int32, @reviews1 array<string>, @reviews2 array<string>, " +
                "@reviews3 array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid1, @title, @category, @prodDate, @duration, @reviews1, @newRelease, @rating)," +
                "(@uuid2, @title, @category, @prodDate, @duration, @reviews2, @newRelease, @rating)," +
                "(@uuid3, @title, @category, @prodDate, @duration, @reviews3, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews1 = ImmutableList.of("Review11!", "Review12!");
        List<String> reviews2 = ImmutableList.of("Review21!", "Review22!");
        List<String> reviews3 = ImmutableList.of("Review31!", "Review32!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid1", "1111").put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950").put("duration", 120).put("reviews1", reviews1)
                .put("reviews2", reviews2).put("reviews3", reviews3).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals(insertedMovie.getUuid(), "1111");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 120);
        Assert.assertEquals(insertedMovie.getReviews(), reviews1);
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        insertedMovie = result.get(1);
        Assert.assertEquals(insertedMovie.getUuid(), "2222");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 120);
        Assert.assertEquals(insertedMovie.getReviews(), reviews2);
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        insertedMovie = result.get(2);
        Assert.assertEquals(insertedMovie.getUuid(), "4444");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 120);
        Assert.assertEquals(insertedMovie.getReviews(), reviews3);
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    /**
     * INSERT the same value to different fields
     *
     * @throws Exception
     */
    @Test
    public void testInsertFieldsWithSameValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "INSERT_TITLE_CATEGORY").put("category", "INSERT_TITLE_CATEGORY").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("INSERT_TITLE_CATEGORY", insertedMovie.getTitle());
        Assert.assertEquals("INSERT_TITLE_CATEGORY", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);
    }

    @Test
    public void testInsertWithUDF() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new MovieSource(), "movieUDF", MovieUDF.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews string, @newRelease boolean, @rating byte);\n" +
                "FROM movieUDF IMPORT parseReviews;\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, parseReviews(@reviews), @newRelease, @rating) " +
                "OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", "Great! Awesome!").put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        List<String> reviews = result.get(0).getReviews();
        Assert.assertEquals(reviews.size(), 2);
        Assert.assertEquals(reviews.get(0), "Great!");
        Assert.assertEquals(reviews.get(1), "Awesome!");
    }

    @Test
    public void testInsertSingleField() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new InsertMovieSourceSingleField()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string);\n" +
                "INSERT INTO source (uuid) values (@uuid) OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("uuid", "1234").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
    }

    @Test
    public void testBatchInsertSingleField() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new InsertMovieSourceSingleField()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @uuid3 string);\n" +
                "INSERT INTO source (uuid) values (@uuid1),(@uuid2),(@uuid3) OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid1", "1111").put("uuid2", "2222").put("uuid3", "4444").build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Assert.assertEquals("1111", result.get(0).getUuid());
        Assert.assertEquals("2222", result.get(1).getUuid());
        Assert.assertEquals("4444", result.get(2).getUuid());
    }


    @Test
    public void testInsertIntoTemporaryTable() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "CREATE TEMPORARY TABLE tempo AS (INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) RETURNING category);\n" +
                "SELECT category FROM tempo OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);
        List<Record> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).get("category"), "Mystery");
    }

    @Test
    public void testAsyncInsert() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new AsyncInsertMovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);
    }

    @Test
    public void testAsyncInsertIntoSelectAsync() throws Exception {
        AsyncInsertMovieSource copyFromMovieSource = new AsyncInsertMovieSource();
        copyFromMovieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        copyFromMovieSource.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x78, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source1", copyFromMovieSource,
                "source2", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("INSERT INTO source2 " +
                "SELECT source1.* FROM source1 " +
                "OUTPUT AS insertedMovies;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Movie> result = rez.getResult("insertedMovies").get().getResult();

        Assert.assertEquals(result.size(), 2, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Awesome!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        insertedMovie = result.get(1);
        Assert.assertEquals("5678", insertedMovie.getUuid());
        Assert.assertEquals("Psycho", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1998", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Scary!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x78);
    }

    @Test
    public void testBatchAsyncInsert() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new AsyncInsertMovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @uuid3 string, @title string," +
                "@category string, @prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid1, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "(@uuid2, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "(@uuid3, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid1", "1111").put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950").put("duration", 120).put("reviews", reviews)
                .put("newRelease", false).put("rating", (byte) 0x7a).build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals(insertedMovie.getUuid(), "1111");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 120);
        Assert.assertEquals(insertedMovie.getReviews(), reviews);
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        insertedMovie = result.get(1);
        Assert.assertEquals(insertedMovie.getUuid(), "2222");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 120);
        Assert.assertEquals(insertedMovie.getReviews(), reviews);
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);

        insertedMovie = result.get(2);
        Assert.assertEquals(insertedMovie.getUuid(), "4444");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 120);
        Assert.assertEquals(insertedMovie.getReviews(), reviews);
        Assert.assertFalse(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);
    }

    /**
     * Ensure that the order of the arguments in the insert clause does not need to match the order of the
     *
     * @throws Exception
     * @Set annotated parameters of the @Insert annotated method.
     */
    @Test
    public void testInsertRandomOrder() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (reviews, category, prodDate, newRelease, duration, title, rating, uuid) " +
                "values (@reviews, @category, @prodDate, @newRelease, @duration, @title, @rating, @uuid) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x7a);
    }

    /**
     * Test projection through 'returning' clause
     *
     * @throws Exception
     */
    @Test
    public void testInsertWithProjection() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "RETURNING uuid, category OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);

        List<Record> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");

        Assert.assertEquals(result.get(0).get("uuid"), "1234");
        Assert.assertEquals(result.get(0).get("category"), "Mystery");

        // Make sure there are no additional fields available beyond the projected ones
        Assert.assertEquals(Iterables.size(result.get(0).getFieldNames()), 2);
    }

    @Test
    public void testBatchInsertWithProjection() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @uuid3 string, @title string, " +
                "@category string, @prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid1, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "(@uuid2, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "(@uuid3, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "RETURNING uuid, category OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid1", "1111").put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950").put("duration", 120).put("reviews", reviews)
                .put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);

        List<Record> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Assert.assertEquals(result.get(0).get("uuid"), "1111");
        Assert.assertEquals(result.get(0).get("category"), "Mystery");

        // Make sure there are no additional fields available beyond the projected ones
        Assert.assertEquals(Iterables.size(result.get(0).getFieldNames()), 2);

        Assert.assertEquals(result.get(1).get("uuid"), "2222");
        Assert.assertEquals(result.get(1).get("category"), "Mystery");
        Assert.assertEquals(result.get(2).get("uuid"), "4444");
        Assert.assertEquals(result.get(2).get("category"), "Mystery");
    }

    /**
     * Assert that for any missing insert arguments (in this example: 'uuid', 'duration', 'reviews', 'newRelease', and 'rating'),
     * the corresponding defaults declared by the @Insert method are used.
     *
     * @throws Exception
     */
    @Test
    public void testInsertWithDefaultValues() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@title string, @category string, @prodDate string);\n" +
                "INSERT INTO source (title, category, prodDate) values (@title, @category, @prodDate) " +
                "OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Movie insertedMovie = result.get(0);
        Assert.assertEquals("DEFAULT_INSERT_UUID", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(122, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);
    }

    /**
     * Assert that for any missing batch insert arguments (in this example: 'duration', 'reviews', 'newRelease', and 'rating'),
     * the corresponding defaults declared by the @Insert method are used.
     *
     * @throws Exception
     */
    @Test
    public void testBatchInsertWithDefaultValues() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @uuid3 string, @title string," +
                "@category string, @prodDate string);\n" +
                "INSERT INTO source (uuid, title, category, prodDate) " +
                "values (@uuid1, @title, @category, @prodDate),(@uuid2, @title, @category, @prodDate)," +
                "(@uuid3, @title, @category, @prodDate) " +
                "OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid1", "1111").put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950")
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals(insertedMovie.getUuid(), "1111");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);

        insertedMovie = result.get(1);
        Assert.assertEquals(insertedMovie.getUuid(), "2222");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);

        insertedMovie = result.get(2);
        Assert.assertEquals(insertedMovie.getUuid(), "4444");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);
    }

    /*
     * First element in the sequence of batch inserts specifies 'null' as its 'uuid' - assert that the inserted record has the
     * default uuid ('DEFAULT_INSERT_UUID') assigned to it
     */
    @Test
    public void testBatchInsertReplaceNullWithDefaultValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid2 string, @uuid3 string, @title string," +
                "@category string, @prodDate string);\n" +
                "INSERT INTO source (uuid, title, category, prodDate) " +
                "values (null, @title, @category, @prodDate),(@uuid2, @title, @category, @prodDate)," +
                "(@uuid3, @title, @category, @prodDate) " +
                "OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950")
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals(insertedMovie.getUuid(), "DEFAULT_INSERT_UUID");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);

        insertedMovie = result.get(1);
        Assert.assertEquals(insertedMovie.getUuid(), "2222");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);

        insertedMovie = result.get(2);
        Assert.assertEquals(insertedMovie.getUuid(), "4444");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);
    }

    /*
     * First element in the sequence of batch inserts specifies "null" string as its 'uuid' - assert that the 'uuid' field of
     * the inserted record equals "null"
     */
    @Test
    public void testBatchInsertNullStringFieldValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid2 string, @uuid3 string, @title string," +
                "@category string, @prodDate string);\n" +
                "INSERT INTO source (uuid, title, category, prodDate) " +
                "values (\"null\", @title, @category, @prodDate),(@uuid2, @title, @category, @prodDate)," +
                "(@uuid3, @title, @category, @prodDate) " +
                "OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950")
                .build(), true);

        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 3, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals(insertedMovie.getUuid(), "null");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);

        insertedMovie = result.get(1);
        Assert.assertEquals(insertedMovie.getUuid(), "2222");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);

        insertedMovie = result.get(2);
        Assert.assertEquals(insertedMovie.getUuid(), "4444");
        Assert.assertEquals(insertedMovie.getTitle(), "Vertigo");
        Assert.assertEquals(insertedMovie.getCategory(), "Mystery");
        Assert.assertEquals(insertedMovie.getProdDate(), "1950");
        Assert.assertEquals(insertedMovie.getDuration().intValue(), 122);
        Assert.assertEquals(insertedMovie.getReviews(), ImmutableList.of("DEFAULT_INSERT_REVIEW_1", "DEFAULT_INSERT_REVIEW_2"));
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x22);
    }

    /*
     * First element in the sequence of batch inserts specifies 'null' as its 'title' - assert that runtime exception
     * is thrown because the mapped-to @Insert method does not declare any default value for this record field
     */
    @Test(expectedExceptions = {ExecutionException.class, IllegalArgumentException.class}, expectedExceptionsMessageRegExp = ".*com.yahoo.yqlplus.engine.sources.MovieSource::insertMovie Missing required property 'title' [(]java.lang.String[)]")
    public void testBatchInsertExceptionMissingDefaultValueForNull() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@title string, @category string, @prodDate string);\n" +
                "INSERT INTO source (uuid, title, category, prodDate) " +
                "values ('1111', null, @category, @prodDate),('2222', @title, @category, @prodDate)," +
                "('4444', @title, @category, @prodDate) " +
                "OUTPUT AS out;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of("title", "title", "category", "category", "prodDate", "prodDate"), true);
        Assert.assertTrue((boolean)rez.getResult("out").get().getResult()); // should not pass (.get() throws)
    }

    /**
     * Assert that if @Insert annotated method declares @DefaultValue annotated parameter that is not also annotated
     * with @Set, a ProgramCompileException will be thrown
     *
     * @throws Exception
     */
    @Test(expectedExceptions = {YQLTypeException.class}, expectedExceptionsMessageRegExp = "method error: com.yahoo.yqlplus.engine.sources.MovieSourceDefaultValueWithoutSet.(insertMovie|updateMovie): .*")
    public void testInsertDefaultValueWithoutSet() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new MovieSourceDefaultValueWithoutSet()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
    }

    /**
     * Assert that ProgramCompileException is thrown if insert argument ('title' in this case) is missing, and the
     * @Insert method does not declare any default for it
     *
     * @throws Exception
     */
    @Test(expectedExceptions = {ExecutionException.class, IllegalArgumentException.class}, expectedExceptionsMessageRegExp = ".*com.yahoo.yqlplus.engine.sources.MovieSource::insertMovie Missing required property 'title' [(]java.lang.String[)]")
    public void testMissingInsertArgumentWithoutDefaultValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>);\n" +
                "INSERT INTO source (uuid, category, prodDate, duration, reviews) " +
                "values (@uuid, @category, @prodDate, @duration, @reviews) " +
                "OUTPUT AS out;");
       ProgramResult rez = program.run(ImmutableMap.<String, Object>builder()
               .put("uuid", "u1")
               .put("title", "title")
               .put("category", "category")
               .put("prodDate", "prodDate")
               .put("duration", 30)
               .put("reviews", ImmutableList.of())
               .build(), true);
        Assert.assertTrue((boolean)rez.getResult("out").get().getResult()); // should not pass (.get() throws)
    }

    /**
     * Assert that ProgramCompileException is thrown if insert argument ('title' in this case) is missing, and the
     * @Insert method does not declare any default for it
     *
     * @throws Exception
     */
    @Test(expectedExceptions = {ExecutionException.class, IllegalArgumentException.class}, expectedExceptionsMessageRegExp = ".*com.yahoo.yqlplus.engine.sources.MovieSource::insertMovie Missing required property 'title' [(]java.lang.String[)]")
    public void testMissingBatchInsertArgumentWithoutDefaultValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @title string, @category string, " +
                    "@prodDate string, @duration int32, @reviews array<string>);\n" +
                    "INSERT INTO source (uuid, category, prodDate, duration, reviews) " +
                    "values (@uuid1, @category, @prodDate, @duration, @reviews),(@uuid2, @category, @prodDate, @duration, @reviews) " +
                    "OUTPUT AS out;");
        program.run(ImmutableMap.<String,Object>builder()
                .put("uuid1", "u1")
                .put("uuid2", "u2")
                .put("title", "title")
                .put("category", "category")
                .put("prodDate", "prodDate")
                .put("duration", 30)
                .put("reviews", ImmutableList.of())
                .build(), true).getResult("out").get();
    }

    /**
     * Assert that ProgramCompileException is thrown if @Insert method declares more than one @Set annotated parameter
     * of the same name.
     *
     * @throws Exception
     */
    @Test(expectedExceptions = YQLTypeException.class, expectedExceptionsMessageRegExp = ".*?InsertSourceWithDuplicateSetParameters.insertMovie: @Set[(]'prodDate'[)] used multiple times")
    public void testInsertSourceWithDuplicateSetParameters() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new InsertSourceWithDuplicateSetParameters()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("PROGRAM (@uuid string, @title string, @category string, @prodDate string, @duration int32, " +
                "@reviews array<string>);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews) " +
                "OUTPUT AS out;");
    }

    /**
     * Assert that ProgramCompileException is thrown if insert source declares more than one @Insert annotated method.
     *
     * @throws Exception
     */
    @Test(expectedExceptions = YQLTypeException.class, expectedExceptionsMessageRegExp = ".*?There can be only one [@]Insert method [(]and one is already set[)]")
    public void testInsertSourceWithMultipleInsertMethods() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new InsertSourceWithMultipleInsertMethods()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("PROGRAM (@uuid string, @title string, @category string, @prodDate string, @duration int32, " +
                "@reviews array<string>);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews) " +
                "OUTPUT AS out;");
    }

    /**
     * Assert that ProgramCompileException is thrown if attempt is made to insert a field ('extra' in this case)
     * for which there is no matching @Set annotated parameter
     *
     * @throws Exception
     */
    @Test(expectedExceptions = {ExecutionException.class, IllegalArgumentException.class}, expectedExceptionsMessageRegExp = ".*com.yahoo.yqlplus.engine.sources.MovieSource::insertMovie Unexpected additional property 'extra' [(]java.lang.String[)]")
    public void testInsertFieldWithoutMatchingSet() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, @prodDate string, @duration int32, " +
                "@reviews array<string>, @extra string);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, extra) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @extra) " +
                "OUTPUT AS out;");
        program.run(ImmutableMap.<String,Object>builder()
                .put("uuid", "u1")
                .put("title", "title")
                .put("category", "category")
                .put("prodDate", "prodDate")
                .put("duration", 30)
                .put("reviews", ImmutableList.of())
                .put("extra", "extra")
                .build(), true).getResult("out").get();
    }

    /**
     * Assert that ProgramCompileException is thrown if @Insert annotated method declares a parameter that is not
     *
     * @throws Exception
     * @Set annotated
     */
    @Test
    public void testInsertMissingSetAnnotation() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source",
                new InsertSourceMissingSetAnnotation()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("PROGRAM (@uuid string, @title string, @category string, @prodDate string, @duration int32, " +
                "@reviews array<string>);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews) " +
                "OUTPUT AS out;");
    }

    @Test
    public void testInsertIntoSelectWithProjection() throws Exception {
        MovieSource copyFromMovieSource = new MovieSource();
        copyFromMovieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        copyFromMovieSource.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x78, "Quentin Tarantino, Kate Winslet");
        MovieSource copyToSource = new MovieSource();
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), 0);
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source1", copyFromMovieSource,
                "source2", copyToSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("INSERT INTO source2 " +
                "SELECT uuid, title, category, prodDate, duration, reviews, newRelease, rating, cast FROM source1 " +
                "OUTPUT AS insertedMovies;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Movie> result = rez.getResult("insertedMovies").get().getResult();

        Assert.assertEquals(result.size(), 2, "Wrong number of movie records inserted");
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), result.size());

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Awesome!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        insertedMovie = result.get(1);
        Assert.assertEquals("5678", insertedMovie.getUuid());
        Assert.assertEquals("Psycho", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1998", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Scary!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x78);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    @Test
    public void testInsertIntoSelectCoerceToRecord() throws Exception {
        MovieSource copyFromMovieSource = new MovieSource();
        copyFromMovieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        copyFromMovieSource.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x78, "Quentin Tarantino, Kate Winslet");
        MovieSource copyToSource = new MovieSource();
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), 0);
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source1", copyFromMovieSource,
                "source2", copyToSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("INSERT INTO source2 " +
                "SELECT source1.* FROM source1 " +
                "OUTPUT AS insertedMovies;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Movie> result = rez.getResult("insertedMovies").get().getResult();

        Assert.assertEquals(result.size(), 2, "Wrong number of movie records inserted");
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), result.size());

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Awesome!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        insertedMovie = result.get(1);
        Assert.assertEquals("5678", insertedMovie.getUuid());
        Assert.assertEquals("Psycho", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1998", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Scary!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x78);
    }

    @Test
    public void testInsertIntoMerge() throws Exception {
        MovieSource copyFromMovieSource2 = new MovieSource();
        copyFromMovieSource2.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x78, "Quentin Tarantino, Kate Winslet");
        MovieSource copyFromMovieSource1 = new MovieSource();
        copyFromMovieSource1.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
                MovieSource copyToSource = new MovieSource();
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), 0);
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source1", copyFromMovieSource1,
                "source2", copyFromMovieSource2, "source3", copyToSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("INSERT INTO source3 " +
                "SELECT * FROM (SELECT source1.* FROM source1 MERGE SELECT source2.* FROM source2) u1 ORDER BY u1.uuid " +
                "OUTPUT AS insertedMergedMovies;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Movie> result = rez.getResult("insertedMergedMovies").get().getResult();

        Assert.assertEquals(result.size(), 2, "Wrong number of movie records inserted");
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), result.size());

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Awesome!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        insertedMovie = result.get(1);
        Assert.assertEquals("5678", insertedMovie.getUuid());
        Assert.assertEquals("Psycho", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1998", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Scary!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x78);
    }

    @Test
    public void testChainedInsert() throws Exception {
        MovieSource source1 = new MovieSource();
        MovieSource source2 = new MovieSource();
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source1", source1, "source2", source2));
        Assert.assertEquals(Iterables.size(source1.getMovies()), 0);
        Assert.assertEquals(Iterables.size(source2.getMovies()), 0);

        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source1 " +
                "INSERT INTO source2 (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "RETURNING uuid, title, category, prodDate, duration, reviews, newRelease, rating, cast " +
                "OUTPUT AS insertedMovie;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult rez = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", true).put("rating", (byte) 0x12)
                .build(), true);

        List<Movie> result = rez.getResult("insertedMovie").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");
        Assert.assertEquals(Iterables.size(source1.getMovies()), 1);
        Assert.assertEquals(Iterables.size(source2.getMovies()), 1);

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);
        Assert.assertEquals(insertedMovie.getCast(), "Various");

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    @Test
    public void testInsertIntoSelectTemporaryTable() throws Exception {
        MovieSource copyFromMovieSource = new MovieSource();
        copyFromMovieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        copyFromMovieSource.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x78, "Quentin Tarantino, Kate Winslet");
        MovieSource copyToSource = new MovieSource();
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), 0);
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source1", copyFromMovieSource,
                "source2", copyToSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("CREATE TEMPORARY TABLE movies AS (SELECT * FROM source1); " +
                "INSERT INTO source2 SELECT movies.* FROM movies " +
                "OUTPUT AS insertedMovies;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Movie> result = rez.getResult("insertedMovies").get().getResult();

        Assert.assertEquals(result.size(), 2, "Wrong number of movie records inserted");
        Assert.assertEquals(Iterables.size(copyToSource.getMovies()), result.size());

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Awesome!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        insertedMovie = result.get(1);
        Assert.assertEquals("5678", insertedMovie.getUuid());
        Assert.assertEquals("Psycho", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1998", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(ImmutableList.of("Great!", "Scary!"), insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x78);
    }

    @Test
    public void testDelete() throws Exception {
        MovieSource movieSource = new MovieSource();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, reviews, true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM source WHERE uuid = '1234' OUTPUT AS deleted;");
        ProgramResult myResult = program.run(null, true);

        // Missing RETURNING clause causes Class to be maintained
        List<Movie> result = myResult.getResult("deleted").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records deleted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
    }

    @Test
    public void testDeleteWithProjection() throws Exception {
        MovieSource movieSource = new MovieSource();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM source WHERE uuid = '1234' RETURNING category, duration OUTPUT AS deleted;");
        ProgramResult myResult = program.run(null, true);
        // RETURNING clause causes coercion to Record
        List<Record> result = myResult.getResult("deleted").get().getResult();
        Assert.assertEquals(result.get(0).get("category"), "Mystery");
        Assert.assertEquals(result.get(0).get("duration"), 120);

        // Make sure there are no additional fields available beyond the projected ones
        Assert.assertEquals(Iterables.size(result.get(0).getFieldNames()), 2);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
    }

    @Test
    public void testDeleteAll() throws Exception {
        MovieSource movieSource = new MovieSource();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        movieSource.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM source RETURNING title OUTPUT AS out;");
        ProgramResult myResult = program.run(null, true);
        List<Record> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 2);
        Assert.assertEquals(result.get(0).get("title"), "Vertigo");
        Assert.assertEquals(result.get(1).get("title"), "Psycho");

        // Make sure there are no additional fields available beyond the projected ones
        Assert.assertEquals(Iterables.size(result.get(0).getFieldNames()), 1);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
    }

    @Test
    public void testUpdate() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews array<string>);\n" +
                "UPDATE source SET (category, duration, reviews) = (@updateCategory, @updateDuration, @updateReviews) where uuid = @uuid OUTPUT AS out;");
        List<String> updateReviews = ImmutableList.of("UPDATE_REVIEW_1", "UPDATE_REVIEW_2");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", updateReviews)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(0).getDuration().intValue(), 90);
        Assert.assertEquals(result.get(0).getReviews(), updateReviews);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    /**
     * UPDATE different fields with the same value
     *
     * @throws Exception
     */
    @Test
    public void testUpdateFieldsWithSameValue() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateTitle string, @updateCategory string, " +
                "@updateDuration int32, @updateReviews array<string>);\n" +
                "UPDATE source SET (title, category, duration, reviews) = (@updateTitle, @updateCategory, @updateDuration, @updateReviews) " +
                "where uuid = @uuid OUTPUT AS out;");
        List<String> updateReviews = ImmutableList.of("UPDATE_REVIEW_1", "UPDATE_REVIEW_2");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateTitle", "UPDATE_TITLE_CATEGORY").put("updateCategory", "UPDATE_TITLE_CATEGORY")
                .put("updateDuration", 90).put("updateReviews", updateReviews)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(0).getDuration().intValue(), 90);
        Assert.assertEquals(result.get(0).getReviews(), updateReviews);
    }

    @Test
    public void testUpdateSetNamedSource() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source.set", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews array<string>);\n" +
                "UPDATE source.set SET (category, duration, reviews) = (@updateCategory, @updateDuration, @updateReviews) where uuid = @uuid OUTPUT AS out;");
        List<String> updateReviews = ImmutableList.of("UPDATE_REVIEW_1", "UPDATE_REVIEW_2");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", updateReviews)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(0).getDuration().intValue(), 90);
        Assert.assertEquals(result.get(0).getReviews(), updateReviews);
    }

    /**
     * Assert that ProgramCompileException is thrown if attempt is made to update a field ('extra' in this case)
     * for which there is no matching @Set annotated parameter
     *
     * @throws Exception
     */
    @Test(expectedExceptions = ProgramCompileException.class, expectedExceptionsMessageRegExp = ".*unknown field 'extra' for method class com.yahoo.yqlplus.engine.sources.MovieSource:updateMovie")
    public void testUpdateFieldWithoutMatchingSet() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, @extra string);\n" +
                "UPDATE source SET (category, duration, extra) = (@updateCategory, @updateDuration, @extra) " +
                "where uuid = @uuid OUTPUT AS out;");
    }

    /**
     * Assert that for any missing update arguments (in this case: 'category'), the corresponding defaults
     * (in this case: 'DEFAULT_UPDATE_CATEGORY') declared by the @Update method are used.
     *
     * @throws Exception
     */
    @Test
    public void testUpdateWithDefaultValues() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Documentary", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateDuration int32);\n" +
                "UPDATE source SET (duration) = (@updateDuration) where uuid = @uuid OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateDuration", 90).build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getCategory(), "DEFAULT_UPDATE_CATEGORY");
        Assert.assertEquals(result.get(0).getDuration().intValue(), 90);
        Assert.assertEquals(result.get(0).getReviews(), ImmutableList.of("DEFAULT_UPDATE_REVIEW_1", "DEFAULT_UPDATE_REVIEW_2"));
    }

    /**
     * Assert that if @Update annotated method declares @DefaultValue annotated parameter that is not also annotated
     * with @Set, a ProgramCompileException will be thrown
     *
     * @throws Exception
     */
    @Test(expectedExceptions = YQLTypeException.class, expectedExceptionsMessageRegExp = ".*com.yahoo.yqlplus.engine.sources.MovieSourceDefaultValueWithoutSet.*")
    public void testUpdateDefaultValueWithoutSet() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new MovieSourceDefaultValueWithoutSet()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        compiler.compile("PROGRAM (@uuid string, @updateDuration int32);\n" +
                "UPDATE source SET (duration) = (@updateDuration) where uuid = @uuid OUTPUT AS out;");
    }

    @Test
    public void testUpdateAllRecordsSingleSetArgument() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Documentary", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        writeSource.insertMovie("5678", "Fargo", "Documentary", "1998", 180, ImmutableList.of("Scary"), false, (byte) 0x42,
                "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateCategory string);\n" +
                "UPDATE source SET (category) = (@updateCategory) OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("updateCategory", "UPDATE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_CATEGORY");

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    @Test
    public void testUpdateAllRecordsMultiSetArguments() throws Exception {
        UpdateMovieSource writeSource = new UpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        writeSource.insertMovie(new Movie("5678", "Fargo", "Documentary", "1998", 180,
                ImmutableList.of("Scary"), false, (byte) 0x42, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateTitle string, @updateCategory string);\n" +
                "UPDATE source SET (title, category) = (@updateTitle, @updateCategory) OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("updateTitle", "UPDATE_TITLE").put("updateCategory", "UPDATE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_CATEGORY");
    }

    @Test
    public void testUpdateAllRecordsDifferentFieldsSameValue() throws Exception {
        UpdateMovieSource writeSource = new UpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        writeSource.insertMovie(new Movie("5678", "Fargo", "Documentary", "1998", 180,
                ImmutableList.of("Scary"), false, (byte) 0x42, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateTitle string, @updateCategory string);\n" +
                "UPDATE source SET (title, category) = (@updateTitle, @updateCategory) OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("updateTitle", "UPDATE_TITLE_CATEGORY").put("updateCategory", "UPDATE_TITLE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getTitle(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_TITLE_CATEGORY");
    }

    @Test
    public void testBatchUpdate() throws Exception {
        UpdateMovieSource writeSource = new UpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        writeSource.insertMovie(new Movie("5678", "Fargo", "Documentary", "1998", 180,
                ImmutableList.of("Scary"), false, (byte) 0x42, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateTitle string, @updateCategory string);\n" +
                "UPDATE source SET (title, category) = (@updateTitle, @updateCategory) WHERE uuid IN ('1234', '5678') OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("updateTitle", "UPDATE_TITLE").put("updateCategory", "UPDATE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_CATEGORY");
    }

    /**
     * Batch UPDATE different fields with the same value
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateFieldsWithSameValue() throws Exception {
        UpdateMovieSource writeSource = new UpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        writeSource.insertMovie(new Movie("5678", "Fargo", "Documentary", "1998", 180,
                ImmutableList.of("Scary"), false, (byte) 0x42, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateTitle string, @updateCategory string);\n" +
                "UPDATE source SET (title, category) = (@updateTitle, @updateCategory) WHERE uuid IN ('1234', '5678') OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("updateTitle", "UPDATE_TITLE_CATEGORY").put("updateCategory", "UPDATE_TITLE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getTitle(), "UPDATE_TITLE_CATEGORY");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_TITLE_CATEGORY");
    }

    @Test
    public void testAsyncUpdate() throws Exception {
        AsyncUpdateMovieSource writeSource = new AsyncUpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateTitle string, @updateCategory string);\n" +
                "UPDATE source SET (title, category) = (@updateTitle, @updateCategory) where uuid = @uuid OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateTitle", "UPDATE_TITLE").put("updateCategory", "UPDATE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Movie updatedMovie = result.get(0);
        Assert.assertEquals("1234", updatedMovie.getUuid());
        Assert.assertEquals("UPDATE_TITLE", updatedMovie.getTitle());
        Assert.assertEquals("UPDATE_CATEGORY", updatedMovie.getCategory());
    }

    /**
     * Assert that the @Key annotated parameter can appear anywhere in an @Update annotated method (does not have to be
     * defined last in the method's parameter list)
     *
     * @throws Exception
     */
    @Test
    public void testUpdateWithUnsortedParameters() throws Exception {
        UpdateMovieSourceWithUnsortedParameters writeSource = new UpdateMovieSourceWithUnsortedParameters();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Mystery", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews array<string>);\n" +
                "UPDATE source SET (category, duration, reviews) = (@updateCategory, @updateDuration, @updateReviews) " +
                "where uuid = @uuid OUTPUT AS out;");
        List<String> updateReviews = ImmutableList.of("UPDATE_REVIEW_1", "UPDATE_REVIEW_2");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", updateReviews)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(0).getDuration().intValue(), 90);
        Assert.assertEquals(result.get(0).getReviews(), updateReviews);
    }
    
    @Test
    public void testUpdateWithConstantAndVariable() throws Exception {
        UpdateMovieSource writeSource = new UpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        writeSource.insertMovie(new Movie("5678", "Fargo", "Documentary", "1998", 180,
                ImmutableList.of("Scary"), false, (byte) 0x42, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateCategory string);\n" +
                "UPDATE source SET (title, category) = ('UPDATE_TITLE', @updateCategory) WHERE uuid IN ('1234', '5678') OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("updateCategory", "UPDATE_CATEGORY").build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_CATEGORY");
    }
    
    @Test
    public void testUpdateWithConstant() throws Exception {
        UpdateMovieSource writeSource = new UpdateMovieSource();
        writeSource.insertMovie(new Movie("1234", "Vertigo", "Documentary", "1950", 120,
                ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, "Quentin Tarantino, Kate Winslet"));
        writeSource.insertMovie(new Movie("5678", "Fargo", "Documentary", "1998", 180,
                ImmutableList.of("Scary"), false, (byte) 0x42, "Quentin Tarantino, Kate Winslet"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "UPDATE source SET (title, category) = ('UPDATE_TITLE', 'UPDATE_CATEGORY') WHERE uuid IN ('1234', '5678') OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), "1234");
        Assert.assertEquals(result.get(0).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(1).getUuid(), "5678");
        Assert.assertEquals(result.get(1).getTitle(), "UPDATE_TITLE");
        Assert.assertEquals(result.get(1).getCategory(), "UPDATE_CATEGORY");
    }

    @Test
    public void testUpdateFieldNameValuePairs() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews array<string>);\n" +
                "UPDATE source SET category = @updateCategory, duration = @updateDuration, reviews = @updateReviews " +
                "where uuid = @uuid OUTPUT AS out;");
        List<String> updateReviews = ImmutableList.of("UPDATE_REVIEW_1", "UPDATE_REVIEW_2");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", updateReviews)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getCategory(), "UPDATE_CATEGORY");
        Assert.assertEquals(result.get(0).getDuration().intValue(), 90);
        Assert.assertEquals(result.get(0).getReviews(), updateReviews);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
    }

    @Test
    public void testUpdateWithUDF() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", writeSource, "movieUDF", MovieUDF.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews string);\n" +
                "FROM movieUDF IMPORT parseReviews;\n" +
                "UPDATE source SET (category, duration, reviews) = (@updateCategory, @updateDuration, parseReviews(@updateReviews)) " +
                "where uuid = @uuid OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", "UPDATE_REVIEW_1 UPDATE_REVIEW_2")
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        List<String> reviews = result.get(0).getReviews();
        Assert.assertEquals(reviews.size(), 2);
        Assert.assertEquals(reviews.get(0), "UPDATE_REVIEW_1");
        Assert.assertEquals(reviews.get(1), "UPDATE_REVIEW_2");
    }

    @Test
    public void testUpdateFieldNameValuePairsWithUDF() throws Exception {
        MovieSource writeSource = new MovieSource();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", writeSource, "movieUDF", MovieUDF.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews string);\n" +
                "FROM movieUDF IMPORT parseReviews;\n" +
                "UPDATE source SET category = @updateCategory, duration = @updateDuration, reviews = parseReviews(@updateReviews) " +
                "where uuid = @uuid OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", "UPDATE_REVIEW_1 UPDATE_REVIEW_2")
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        List<String> reviews = result.get(0).getReviews();
        Assert.assertEquals(reviews.size(), 2);
        Assert.assertEquals(reviews.get(0), "UPDATE_REVIEW_1");
        Assert.assertEquals(reviews.get(1), "UPDATE_REVIEW_2");
    }

    /**
     * Test for OUTPUT COUNT AS directive.
     *
     * @throws Exception
     */
    @Test
    public void testOutputCountAs() throws Exception {
        // Non-empty
        MovieSource movieSource = new MovieSource();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        movieSource.insertMovie("5678", "Psycho", "Mystery", "1998", 120, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM source OUTPUT COUNT AS numDeleted;");
        ProgramResult myResult = program.run(null, true);
        Assert.assertEquals((int) myResult.getResult("numDeleted").get().getResult(), 2);

        // Empty
        movieSource = new MovieSource();
        injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSource));
        compiler = injector.getInstance(YQLPlusCompiler.class);
        program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM source OUTPUT COUNT AS numDeleted;");
        myResult = program.run(null, true);
        Assert.assertEquals((int) myResult.getResult("numDeleted").get().getResult(), 0);
    }

    @Test
    public void testBatchInsertOutputCountAs() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid1 string, @uuid2 string, @uuid3 string, @title string," +
                "@category string, @prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid1, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "(@uuid2, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating)," +
                "(@uuid3, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT COUNT AS numInserted;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid1", "1111").put("uuid2", "2222").put("uuid3", "4444").put("title", "Vertigo")
                .put("category", "Mystery").put("prodDate", "1950").put("duration", 120).put("reviews", reviews)
                .put("newRelease", false).put("rating", (byte) 0x7a).build(), true);
        Assert.assertEquals((int)myResult.getResult("numInserted").get().getResult(), 3);
    }

    @Test
    public void testLongKey() throws Exception {
        Long uuid = Long.valueOf("1234");
        MovieSourceWithLongUuid movieSourceWithLongUuid = new MovieSourceWithLongUuid();
        movieSourceWithLongUuid.insertMovie(new MovieSourceWithLongUuid.Movie(uuid, "Vertigo", "Mystery"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSourceWithLongUuid));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid int64);\n" +
                "SELECT * FROM source WHERE uuid = @uuid OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("uuid", uuid).build(), true);
        List<MovieSourceWithLongUuid.Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), uuid);
        Assert.assertEquals(result.get(0).getTitle(), "Vertigo");
        Assert.assertEquals(result.get(0).getCategory(), "Mystery");
    }

    @Test
    public void testInsertWithMetricEmitter() throws Exception {
        MetricModule metricModule = new MetricModule(new MetricDimension().with("key1", "value1").with("key2", "value2"), true);
        JavaTestModule javaTestModule = new JavaTestModule(metricModule);
        Injector injector = Guice.createInjector(javaTestModule, new SourceBindingModule("movieSourceWithEmitter", new MovieSourceWithMetricEmitter()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO movieSourceWithEmitter (uuid, title, category, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        StandardRequestEmitter requestEmitter =  metricModule.getStandardRequestEmitter();
        ExecutionScope scope = new MapExecutionScope()
            .bind(TaskMetricEmitter.class, requestEmitter.start("program", "<string>"));
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true, scope).getEnd().get();
        requestEmitter.complete();
        RequestEvent requestEvent = javaTestModule.getRequestEvent();
        Queue<RequestMetric> metrics = requestEvent.getMetrics();
        boolean foundTask = false;
        boolean foundLatency = false;
        for (RequestMetric metric : metrics) {
            if (",subtask=createResponse,method=insertMovie,source=movieSourceWithEmitter,query=out,program=<string>,key2=value2,key1=value1".equals(dimensionStr(metric.getMetric().getDimension()))) {
                foundTask = true;
            }
            if (metric.getMetric().getType() == MetricType.DURATION && metric.getMetric().getName().equals("requestLatency")) {
                foundLatency = true;
            }
        }
        Assert.assertTrue(foundTask && foundLatency);
    }

    @Test
    public void testUpdateWithMetricEmitter() throws Exception {
        MovieSourceWithMetricEmitter writeSource = new MovieSourceWithMetricEmitter();
        writeSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, null);
        MetricModule metricModule = new MetricModule(new MetricDimension().with("key1", "value1").with("key2", "value2"), true);
        JavaTestModule javaTestModule = new JavaTestModule(metricModule);
        Injector injector = Guice.createInjector(javaTestModule, new SourceBindingModule("movieSourceWithEmitter", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @updateCategory string, @updateDuration int32, " +
                "@updateReviews array<string>);\n" +
                "UPDATE movieSourceWithEmitter SET (category, duration, reviews) = (@updateCategory, @updateDuration, @updateReviews) where uuid = @uuid OUTPUT AS out;");
        StandardRequestEmitter requestEmitter =  metricModule.getStandardRequestEmitter();
        ExecutionScope scope = new MapExecutionScope()
            .bind(TaskMetricEmitter.class, requestEmitter.start("program", "<string>"));
        List<String> updateReviews = ImmutableList.of("UPDATE_REVIEW_1", "UPDATE_REVIEW_2");
        program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("updateCategory", "UPDATE_CATEGORY").put("updateDuration", 90)
                .put("updateReviews", updateReviews)
                .build(), true, scope).getEnd().get();
        RequestEvent requestEvent = javaTestModule.getRequestEvent();
        Queue<RequestMetric> metrics = requestEvent.getMetrics();
        boolean foundTask = false;
        boolean foundLatency = false;
        for (RequestMetric metric : metrics) {
            if (",subtask=createResponse,method=updateMovie,source=movieSourceWithEmitter,query=out,program=<string>,key2=value2,key1=value1".equals(dimensionStr(metric.getMetric().getDimension()))) {
                foundTask = true;
            }
            if (metric.getMetric().getType() == MetricType.DURATION && metric.getMetric().getName().equals("requestLatency")) {
                foundLatency = true;
            }
        }
        Assert.assertTrue(foundTask && foundLatency);
    }

    @Test
    public void testUpdateAllRecordsWithMetricEmitter() throws Exception {
        MovieSourceWithMetricEmitter writeSource = new MovieSourceWithMetricEmitter();
        writeSource.insertMovie("1234", "Vertigo", "Documentary", "1950", 120, ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, null);
        MetricModule metricModule = new MetricModule(new MetricDimension().with("key1", "value1").with("key2", "value2"), true);
        JavaTestModule javaTestModule = new JavaTestModule(metricModule);
        Injector injector = Guice.createInjector(javaTestModule, new SourceBindingModule("movieSourceWithEmitter", writeSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@updateCategory string);\n" +
                "UPDATE movieSourceWithEmitter SET (category) = (@updateCategory) OUTPUT AS out;");
        StandardRequestEmitter requestEmitter =  metricModule.getStandardRequestEmitter();
        ExecutionScope scope = new MapExecutionScope()
            .bind(TaskMetricEmitter.class, requestEmitter.start("program", "<string>"));
        program.run(new ImmutableMap.Builder<String, Object>().put("updateCategory", "UPDATE_CATEGORY")
                .build(), true, scope).getEnd().get();
        RequestEvent requestEvent = javaTestModule.getRequestEvent();
        Queue<RequestMetric> metrics = requestEvent.getMetrics();
        boolean foundTask = false;
        boolean foundLatency = false;
        for (RequestMetric metric : metrics) {
            if (",subtask=createResponse,method=updateAllMovies,source=movieSourceWithEmitter,query=out,program=<string>,key2=value2,key1=value1".equals(dimensionStr(metric.getMetric().getDimension()))) {
                foundTask = true;
            }
            if (metric.getMetric().getType() == MetricType.DURATION && metric.getMetric().getName().equals("requestLatency")) {
                foundLatency = true;
            }
        }
        Assert.assertTrue(foundTask && foundLatency);
    }

    @Test
    public void testDeleteWithMetricEmitter() throws Exception {
        MovieSourceWithMetricEmitter movieSource = new MovieSourceWithMetricEmitter();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"), true, (byte) 0x12, null);
        MetricModule metricModule = new MetricModule(new MetricDimension().with("key1", "value1").with("key2", "value2"), true);
        JavaTestModule javaTestModule = new JavaTestModule(metricModule);
        Injector injector = Guice.createInjector(javaTestModule, new SourceBindingModule("movieSourceWithEmitter", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM movieSourceWithEmitter WHERE uuid = '1234' RETURNING category, duration OUTPUT AS out;");
        StandardRequestEmitter requestEmitter =  metricModule.getStandardRequestEmitter();
        ExecutionScope scope = new MapExecutionScope()
            .bind(TaskMetricEmitter.class, requestEmitter.start("program", "<string>"));
        program.run(ImmutableMap.<String, Object>of(), true, scope).getEnd().get();
        RequestEvent requestEvent = javaTestModule.getRequestEvent();
        Queue<RequestMetric> metrics = requestEvent.getMetrics();
        boolean foundTask = false;
        boolean foundLatency = false;
        for (RequestMetric metric : metrics) {
            if (",subtask=createResponse,method=deleteMovie,source=movieSourceWithEmitter,query=out,program=<string>,key2=value2,key1=value1".equals(dimensionStr(metric.getMetric().getDimension()))) {
                foundTask = true;
            }
            if (metric.getMetric().getType() == MetricType.DURATION && metric.getMetric().getName().equals("requestLatency")) {
                foundLatency = true;
            }
        }
        Assert.assertTrue(foundTask && foundLatency);
    }

    /**
     * ORDER BY to work on projected field outputs
     *
     * @throws Exception
     */
    @Test
    public void testOrderByProjectionFieldFromUDF() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("cool", UDFsTest.CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT cool.joeify(people.value) AS joeified FROM people ORDER BY joeified OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("joeified").toString(), "bobjoe");
        record = foo.get(1);
        Assert.assertEquals(record.get("joeified").toString(), "joejoe");
        record = foo.get(2);
        Assert.assertEquals(record.get("joeified").toString(), "smithjoe");
    }

    /**
     * ORDER BY to work on projected field outputs
     *
     * @throws Exception
     */
    @Test
    public void testOrderByProjectionFieldFromUDFInDescendingOrder() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("cool", UDFsTest.CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT cool.joeify(people.value) AS joeified FROM people ORDER BY joeified DESC OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("joeified").toString(), "smithjoe");
        record = foo.get(1);
        Assert.assertEquals(record.get("joeified").toString(), "joejoe");
        record = foo.get(2);
        Assert.assertEquals(record.get("joeified").toString(), "bobjoe");
    }

    /**
     * ORDER BY to work on projected field outputs
     *
     * @throws Exception
     */
    @Test
    public void testOrderByProjectionFieldFromUDFApplyLimit() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("cool", UDFsTest.CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT cool.joeify(people.value) AS joeified FROM people ORDER BY joeified LIMIT 1 OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("joeified").toString(), "bobjoe");
    }

    @Test
    public void testOrderBySourceField() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT value FROM people ORDER BY value OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("value").toString(), "bob");
        record = foo.get(1);
        Assert.assertEquals(record.get("value").toString(), "joe");
        record = foo.get(2);
        Assert.assertEquals(record.get("value").toString(), "smith");
    }

    @Test
    public void testOrderBySourceAliasField() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT value AS aliasValue FROM people ORDER BY aliasValue OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("aliasValue").toString(), "bob");
        record = foo.get(1);
        Assert.assertEquals(record.get("aliasValue").toString(), "joe");
        record = foo.get(2);
        Assert.assertEquals(record.get("aliasValue").toString(), "smith");
    }

    @Test
    public void testOrderByUDFSourceField() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("cool", UDFsTest.CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT value FROM people ORDER BY cool.strlen(value) DESC OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("value").toString(), "smith");
        record = foo.get(1);
        Assert.assertEquals(record.get("value").toString(), "bob");
        record = foo.get(2);
        Assert.assertEquals(record.get("value").toString(), "joe");
    }

    @Test
    public void testOrderByUDFSourceAliasField() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("cool", UDFsTest.CoolModule.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "SELECT value AS aliasValue FROM people ORDER BY cool.strlen(aliasValue) DESC OUTPUT AS foo;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("aliasValue").toString(), "smith");
        record = foo.get(1);
        Assert.assertEquals(record.get("aliasValue").toString(), "bob");
        record = foo.get(2);
        Assert.assertEquals(record.get("aliasValue").toString(), "joe");
    }

    @Test
    public void testJoinOrderBySourceFields() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT moreMinions.minion_id, people.score " +
                "FROM people " +
                "JOIN moreMinions ON people.id = moreMinions.master_id " +
                "ORDER BY people.score DESC, moreMinions.minion_id OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("score").toString(), "1");
        Assert.assertEquals(record.get("minion_id").toString(), "1");
        record = foo.get(1);
        Assert.assertEquals(record.get("score").toString(), "0");
        Assert.assertEquals(record.get("minion_id").toString(), "2");
        record = foo.get(2);
        Assert.assertEquals(record.get("score").toString(), "0");
        Assert.assertEquals(record.get("minion_id").toString(), "3");
    }

    @Test
    public void testJoinOrderBySourceFieldsWithOverlappingNames() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.id, citizen.id " +
                "FROM people " +
                "JOIN citizen ON people.id = citizen.id " +
                "ORDER BY people.id DESC OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Record record = foo.get(0);
        Assert.assertEquals(record.get("id").toString(), "3");
        Assert.assertEquals(record.get("id1").toString(), "3");
        record = foo.get(1);
        Assert.assertEquals(record.get("id").toString(), "2");
        Assert.assertEquals(record.get("id1").toString(), "2");
        record = foo.get(2);
        Assert.assertEquals(record.get("id").toString(), "1");
        Assert.assertEquals(record.get("id1").toString(), "1");

        injector = Guice.createInjector(new JavaTestModule());
        compiler = injector.getInstance(YQLPlusCompiler.class);
        program = compiler.compile("SELECT people.id, citizen.id " +
                "FROM people " +
                "JOIN citizen ON people.id = citizen.id " +
                "ORDER BY citizen.id DESC OUTPUT AS foo;");
        myResult = program.run(ImmutableMap.<String, Object>of(), true);
        rez = myResult.getResult("foo").get();
        foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        record = foo.get(0);
        Assert.assertEquals(record.get("id").toString(), "3");
        Assert.assertEquals(record.get("id1").toString(), "3");
        record = foo.get(1);
        Assert.assertEquals(record.get("id").toString(), "2");
        Assert.assertEquals(record.get("id1").toString(), "2");
        record = foo.get(2);
        Assert.assertEquals(record.get("id").toString(), "1");
        Assert.assertEquals(record.get("id1").toString(), "1");
    }

    /**
     * Support automatic coercion to Record when using "SELECT foo.* FROM foo" syntax
     *
     * @throws Exception
     */
    @Test
    public void testCoerceToRecord() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM people OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);

        // Assert that Class is maintained
        Assert.assertTrue(foo.get(0) instanceof Person, "Class " + Person.class.getName() + " not maintained by SELECT *");
        Person person = (Person) foo.get(0);
        Assert.assertEquals(person.getId(), "1");
        Assert.assertEquals(person.getValue(), "bob");
        Assert.assertEquals(person.getScore(), 0);

        program = compiler.compile("SELECT people.* FROM people OUTPUT AS foo;");
        myResult = program.run(ImmutableMap.<String, Object>of(), true);
        rez = myResult.getResult("foo").get();
        foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);

        // Assert that Class is *not* maintained
        Assert.assertFalse(foo.get(0) instanceof Person, "Automatic coercion to Record has not occurred");

        Record record = foo.get(0);
        // Assert record size is 6 (includes all the fields from table "people")
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "joe");
        Assert.assertEquals(record.get("id"), "2");
        Assert.assertEquals(record.get("iid"), 2);
        Assert.assertEquals(record.get("iidPrimitive"), 2);
        Assert.assertEquals(record.get("otherId"), "2");
        Assert.assertEquals(record.get("score"), 1);

        record = foo.get(2);
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "smith");
        Assert.assertEquals(record.get("id"), "3");
        Assert.assertEquals(record.get("iid"), 3);
        Assert.assertEquals(record.get("iidPrimitive"), 3);
        Assert.assertEquals(record.get("otherId"), "3");
        Assert.assertEquals(record.get("score"), 2);
    }

    @Test
    public void testMatchesKeywordAsIdent() throws Exception {
        PersonSource personSource = new PersonSource(ImmutableList.of(new Person("1", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2)));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("people.matches", personSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM people.matches WHERE score = 1 OUTPUT AS matches;\n");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("matches").get();
        List<Person> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Assert.assertEquals(foo.get(0).getId(), "2");
        Assert.assertEquals(foo.get(0).getValue(), "joe");
    }

    @Test
    public void testStaticSelect() throws Exception {
        String programStr =
                "SELECT" + "\n" +
                        "{" + "\n" +
                        "    \"sports\": [" + "\n" +
                        "        {" + "\n" +
                        "            \"code\": \"mlb\"," + "\n" +
                        "            \"abbr\":\"MLB\"," + "\n" +
                        "            \"name\": \"Major League Baseball\"," + "\n" +
                        "            \"iconURL\": \"http://somedomain.com/some/path/to/an/image.png\"," + "\n" +
                        "            \"series\":[" + "\n" +
                        "                {" + "\n" +
                        "                    \"id\":1," + "\n" +
                        "                    \"startDate\":1411667287," + "\n" +
                        "                    \"additionalDescription\":\"Late\"," + "\n" +
                        "                    \"games\": [" + "\n" +
                        "                        {" + "\n" +
                        "                            \"id\": 1," + "\n" +
                        "                            \"homeTeam\": {" + "\n" +
                        "                                \"name\":\"Houston Astros\"," + "\n" +
                        "                                \"code\":\"mlb.t.123\"," + "\n" +
                        "                                \"abbr\":\"HOU\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#FF0000\"," + "\n" +
                        "                                \"wins\":2," + "\n" +
                        "                                \"losses\":2," + "\n" +
                        "                                \"lineupPosted\":false" + "\n" +
                        "                            }," + "\n" +
                        "                            \"awayTeam\": {" + "\n" +
                        "                                \"name\":\"Atlanta Braves\"," + "\n" +
                        "                                \"code\":\"mlb.t.124\"," + "\n" +
                        "                                \"abbr\":\"ATL\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#00FF00\"," + "\n" +
                        "                                \"wins\":4," + "\n" +
                        "                                \"losses\":0," + "\n" +
                        "                                \"lineupPosted\":true" + "\n" +
                        "                            }," + "\n" +
                        "                            \"time\": 545456000" + "\n" +
                        "                        }," + "\n" +
                        "                        {" + "\n" +
                        "                            \"id\": 2," + "\n" +
                        "                            \"homeTeam\": {" + "\n" +
                        "                                \"name\":\"Houston Astros\"," + "\n" +
                        "                                \"code\":\"mlb.t.123\"," + "\n" +
                        "                                \"abbr\":\"HOU\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#FF0000\"," + "\n" +
                        "                                \"wins\":2," + "\n" +
                        "                                \"losses\":2," + "\n" +
                        "                                \"linupPosted\":false" + "\n" +
                        "                            }," + "\n" +
                        "                            \"awayTeam\": {" + "\n" +
                        "                                \"name\":\"Atlanta Braves\"," + "\n" +
                        "                                \"code\":\"mlb.t.124\"," + "\n" +
                        "                                \"abbr\":\"ATL\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#00FF00\"," + "\n" +
                        "                                \"wins\":4," + "\n" +
                        "                                \"losses\":0," + "\n" +
                        "                                \"lineupPosted\":true" + "\n" +
                        "                            }," + "\n" +
                        "                            \"time\": 545456000" + "\n" +
                        "                        }" + "\n" +
                        "                    ]" + "\n" +
                        "                }," + "\n" +
                        "                {" + "\n" +
                        "                    \"startDate\":1412099287," + "\n" +
                        "                    \"additionalDescription\":\"Tues Only\"," + "\n" +
                        "                    \"games\": [" + "\n" +
                        "                        {" + "\n" +
                        "                            \"id\": 1," + "\n" +
                        "                            \"homeTeam\": {" + "\n" +
                        "                                \"name\":\"Houston Astros\"," + "\n" +
                        "                                \"code\":\"mlb.t.123\"," + "\n" +
                        "                                \"abbr\":\"HOU\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#FF0000\"," + "\n" +
                        "                                \"wins\":2," + "\n" +
                        "                                \"losses\":2," + "\n" +
                        "                                \"lineupPosted\":false" + "\n" +
                        "                            }," + "\n" +
                        "                            \"awayTeam\": {" + "\n" +
                        "                                \"name\":\"Atlanta Braves\"," + "\n" +
                        "                                \"code\":\"mlb.t.124\"," + "\n" +
                        "                                \"abbr\":\"ATL\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#00FF00\"," + "\n" +
                        "                                \"wins\":4," + "\n" +
                        "                                \"losses\":0," + "\n" +
                        "                                \"lineupPosted\":true" + "\n" +
                        "                            }," + "\n" +
                        "                            \"time\": 545456000" + "\n" +
                        "                        }," + "\n" +
                        "                        {" + "\n" +
                        "                            \"id\": 2," + "\n" +
                        "                            \"homeTeam\": {" + "\n" +
                        "                                \"name\":\"Houston Astros\"," + "\n" +
                        "                                \"code\":\"mlb.t.123\"," + "\n" +
                        "                                \"abbr\":\"HOU\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#FF0000\"," + "\n" +
                        "                                \"wins\":2," + "\n" +
                        "                                \"losses\":2," + "\n" +
                        "                                \"lineupPosted\":false" + "\n" +
                        "                            }," + "\n" +
                        "                            \"awayTeam\": {" + "\n" +
                        "                                \"name\":\"Atlanta Braves\"," + "\n" +
                        "                                \"code\":\"mlb.t.124\"," + "\n" +
                        "                                \"abbr\":\"ATL\"," + "\n" +
                        "                                \"icon\":\"http://somedomain.com/path/to/icon.png\"," + "\n" +
                        "                                \"color\":\"#00FF00\"," + "\n" +
                        "                                \"wins\":4," + "\n" +
                        "                                \"losses\":0," + "\n" +
                        "                                \"lineupPosted\":true" + "\n" +
                        "                            }," + "\n" +
                        "                            \"time\": 545456000" + "\n" +
                        "                        }" + "\n" +
                        "                    ]" + "\n" +
                        "                }" + "\n" +
                        "            ]" + "\n" +
                        "        }" + "\n" +
                        "    ]" + "\n" +
                        "} AS mocked" + "\n" +
                        "OUTPUT AS contestSeries;" + "\n";
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet result = myResult.getResult("contestSeries").get();
        List<Record> records = result.getResult();
        Object obj = ((Map) records.get(0).get("mocked")).get("sports");
        Assert.assertNotNull(obj);
    }
    
    @Test
    public void testLambda() throws Exception {
        String programStr = "PROGRAM (@id array<string>, @low int32, @high int32);" + "\n"
                          + "SELECT * FROM personList(@low, @high) WHERE id IN (@id) OUTPUT AS persons;";
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(
                new ImmutableMap.Builder<String, Object>()
                        .put("id", Arrays.asList("1", "2", "3", "4", "5"))
                        .put("low", 2).put("high", 4).build(), true);
        List<Record> results = myResult.getResult("persons").get().getResult();
        assertEquals(1, results.size());
    }
    
    @Test
    public void testSimplistWithBoxedParamSourceSource() throws Exception {
        String programStr = "PROGRAM (@category string); \n" +
                            "select id, category \n" +
                            "from sampleListWithBoxedParams(10, 5.0, \"program\") \n" +
                            "where category = @category order by id \n" + 
                            "output as sampleIds; \n";
        Injector injector = Guice.createInjector(new JavaTestModule(), new AbstractModule() {
            @Override
            protected void configure() {
                MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
                sourceBindings.addBinding("sampleListWithBoxedParams").toInstance(new SampleListSourceWithBoxedParams());
            }            
        });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("category", "test").build(), true);
        YQLResultSet result = myResult.getResult("sampleIds").get();
        List resultSet = result.getResult();
        assertEquals(10, resultSet.size());
    }   
    
    @Test
    public void testSimplistWithUnBoxedParamSourceSource() throws Exception {
        String programStr = "PROGRAM (@category string); \n" +
                            "select id, category \n" +
                            "from sampleListWithUnBoxedParams(10, 5.0, \"program\") \n" +
                            "where category = @category order by id \n" + 
                            "output as sampleIds; \n";
        Injector injector = Guice.createInjector(new JavaTestModule(), new AbstractModule() {
            @Override
            protected void configure() {
                MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
                sourceBindings.addBinding("sampleListWithUnBoxedParams").toInstance(new SampleListSourceWithUnboxedParams());
            }            
        });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("category", "test").build(), true);
        YQLResultSet result = myResult.getResult("sampleIds").get();
        List resultSet = result.getResult();
        assertEquals(10, resultSet.size());
    }
    
    @Test
    public void testBoxedLongArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new LongSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@start int64 = -1L,  @end int64 = -2L);\n" +
                "SELECT * FROM source(@start, @end) OUTPUT AS f1;\n");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().build(), true);
        List<LongSource.Sample> f1 = myResult.getResult("f1").get().getResult();
        assertEquals(1, f1.size());
        assertEquals(-1, f1.get(0).getStart());
        assertEquals(-2, f1.get(0).getEnd());
    }
    
    @Test
    public void testUnBoxedLongArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new LongSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@start int64 = -1L,  @end int64 = -2L, @increment boolean = false);\n" +
                "SELECT * FROM source(@start, @end) OUTPUT AS f1;\n");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().build(), true);
        List<LongSource.Sample> f1 = myResult.getResult("f1").get().getResult();
        assertEquals(1, f1.size());
        assertEquals(-1, f1.get(0).getStart());
        assertEquals(-2, f1.get(0).getEnd());
        program = compiler.compile("PROGRAM (@start int64 = -1L,  @end int64 = -2L, @increment boolean = true);\n" +
            "SELECT * FROM source(@start, @end, @increment) OUTPUT AS f1;\n");
        myResult = program.run(new ImmutableMap.Builder<String, Object>().build(), true);
        f1 = myResult.getResult("f1").get().getResult();
        assertEquals(1, f1.size());
        assertEquals(0, f1.get(0).getStart());
        assertEquals(-1, f1.get(0).getEnd());
    }
    
    @Test
    public void testBoxedArgument() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new BoxedParameterSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@b byte = 1, @s int16 = 2, @i int32 = 3 , @l int64 = 4L,  @d double = 5.0, @bb boolean = true);\n" +
                "SELECT * FROM source(@b, @s, @i, @l, @d, @bb) OUTPUT AS f1;\n");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().build(), true);
        List<BoxedParameterSource.Sample> f1 = myResult.getResult("f1").get().getResult();
        assertEquals(1, f1.size());
        assertEquals(1, f1.get(0).getB());
        assertEquals(2, f1.get(0).getS());
        assertEquals(3, f1.get(0).getI());
        assertEquals(4, f1.get(0).getL());      
        assertEquals(5.0, f1.get(0).getD());
        assertTrue(f1.get(0).isBb());
    }

    /*
     * NullPointerException using ORDER BY
     */
    @Test
    public void testOrderByNullField() throws Exception {
        MovieSource movieSource = new MovieSource();
        // Create 2 movie records with "null" duration and ORDER BY that
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", null, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        movieSource.insertMovie("5678", "Psycho", "Mystery", "1998", null, ImmutableList.of("Great!", "Scary!"),
                true, (byte) 0x78, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("movieSource", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM movieSource ORDER BY duration OUTPUT AS movies;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Movie> records = rez.getResult("movies").get().getResult();
        assertEquals(2, records.size());
        assertNull(records.get(0).getDuration());
        assertNull(records.get(1).getDuration());
    }

    /*
     * Test 'select cast from' fails with no viable alternative
     */
    @Test
    public void testCastProperty() throws Exception {
        MovieSource movieSource = new MovieSource();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 45, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("movieSource", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT cast FROM movieSource OUTPUT AS cast;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> records = rez.getResult("cast").get().getResult();
        assertEquals(1, records.size());
        assertEquals("Quentin Tarantino, Kate Winslet", records.get(0).get("cast"));
    }

    @Test(expectedExceptions = DependencyNotFoundException.class, expectedExceptionsMessageRegExp = ".*Source 'testSource' not found")
    public void testMissingDependencyError() throws Exception {
        Long uuid = Long.valueOf("1234");
        MovieSourceWithLongUuid movieSourceWithLongUuid = new MovieSourceWithLongUuid();
        movieSourceWithLongUuid.insertMovie(new MovieSourceWithLongUuid.Movie(uuid, "Vertigo", "Mystery"));
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSourceWithLongUuid));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid int64);\n" +
                "SELECT * FROM testSource WHERE uuid = @uuid OUTPUT AS out;");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("uuid", uuid).build(), true);
        List<MovieSourceWithLongUuid.Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.get(0).getUuid(), uuid);
        Assert.assertEquals(result.get(0).getTitle(), "Vertigo");
        Assert.assertEquals(result.get(0).getCategory(), "Mystery");
    }

    @Test(expectedExceptions=ExecutionException.class,  expectedExceptionsMessageRegExp = "java.lang.NumberFormatException: For input string: \"a,b\"")
    public void testGetRequestTraceException() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("errorSource", new ErrorSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (); SELECT * FROM errorSource OUTPUT AS errorResult;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        TraceRequest traceRequest = rez.getEnd().get();
        traceRequest.getEntries();
    }

    @Test
    public void testJsonArray() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("jsonArray", new JsonArraySource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@count int32); SELECT * FROM jsonArray(@count) OUTPUT AS jsonArray;");
        ProgramResult rez = program.run(new ImmutableMap.Builder<String, Object>().put("count", 3).build(), true);
        List<JsonArraySource.JsonResult> records = rez.getResult("jsonArray").get().getResult();
        assertEquals(1, records.size());
        assertEquals(0, (records.get(0).jsonNode).get(0).asInt());

        program = compiler.compile("PROGRAM (); SELECT * FROM jsonArray OUTPUT AS jsonArray;");
        rez = program.run(new ImmutableMap.Builder<String, Object>().build(), true);
        records = rez.getResult("jsonArray").get().getResult();
        assertEquals(1, records.size());
        assertEquals("textNode", records.get(0).jsonNode.asText());
    }
    
    @Test
    public void testOrderbyNotAlignedFields() throws Exception {
        String programStr = 
                "PROGRAM (@category string); \n" +
                "CREATE TEMP TABLE samples1 AS (SELECT id, category AS category1 \n" +
                "FROM sampleListWithUnBoxedParams(10, 5.0, \"program\") \n" +
                "WHERE category = @category); \n" +
                "CREATE TEMP TABLE samples2 AS (SELECT id, category AS category2 \n" +
                "FROM sampleListWithUnBoxedParams(10, 5.0, \"program\") \n" +
                "WHERE category = @category); \n" +
                "CREATE TEMP TABLE samples AS ( \n" +
                "SELECT * \n" +
                "FROM samples1 \n" +
                "MERGE  \n" +
                "SELECT * \n" +
                "FROM samples2); \n" +
                "SELECT * \n" +
                "FROM samples \n" +
                "ORDER BY id \n" +
                "OUTPUT AS orderedSamples;";
        Injector injector = Guice.createInjector(new JavaTestModule(), new AbstractModule() {
                @Override
                protected void configure() {
                    MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
                    sourceBindings.addBinding("sampleListWithUnBoxedParams").toInstance(new SampleListSourceWithUnboxedParams());
                }            
            });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("category", "test").build(), true);
        YQLResultSet result = myResult.getResult("orderedSamples").get();
        List<Record> resultSet = result.getResult();
        assertEquals(20, resultSet.size());
        assertEquals(0, ((Record)resultSet.get(0)).get("id"));
        assertEquals(0, ((Record)resultSet.get(1)).get("id"));
    }
    
    @Test
    public void testOrderbyNotAlignedFields2() throws Exception {
        String programStr = 
                "PROGRAM (@category string); \n" +
                "CREATE TEMP TABLE samples1 AS (SELECT id, category AS category1 \n" +
                "FROM sampleListWithUnBoxedParams(10, 5.0, \"program\") \n" +
                "WHERE category = @category); \n" +
                "CREATE TEMP TABLE samples2 AS (SELECT id, '' AS category2 \n" +
                "FROM sampleListWithUnBoxedParams(10, 5.0, \"program\") \n" +
                "WHERE category = @category); \n" +
                "CREATE TEMP TABLE samples AS ( \n" +
                "SELECT * \n" +
                "FROM samples1 \n" +
                "MERGE  \n" +
                "SELECT * \n" +
                "FROM samples2); \n" +
                "SELECT * \n" +
                "FROM samples \n" +
                "ORDER BY id\n" +
                "OUTPUT AS orderedSamples;";
        Injector injector = Guice.createInjector(new JavaTestModule(), new AbstractModule() {
                @Override
                protected void configure() {
                    MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
                    sourceBindings.addBinding("sampleListWithUnBoxedParams").toInstance(new SampleListSourceWithUnboxedParams());
                }            
            });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("category", "test").build(), true);
        YQLResultSet result = myResult.getResult("orderedSamples").get();
        List<Record> resultSet = result.getResult();
        assertEquals(20, resultSet.size());
        assertEquals(0, ((Record)resultSet.get(0)).get("id"));
        assertEquals(0, ((Record)resultSet.get(1)).get("id"));
    }
    
    @Test
    public void testArrayArgumentParse() throws Exception {
      String programStr = "PROGRAM(@query array<string>=[]); " +
                          "SELECT @query OUTPUT AS program_arguments;";
      Injector injector = Guice.createInjector(new JavaTestModule());
      YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
      CompiledProgram program = compiler.compile(programStr);
      ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
      List<Record> f1 = rez.getResult("program_arguments").get().getResult();
      Assert.assertEquals((ArrayList)f1.get(0).get("query"), new ArrayList());
    }
    
    @Test
    public void testListDefaultArgumentValue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new KeyedSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM (@strArrayArg1 array<string> = ['array test1'], "
                                   + "@strArrayArg2 array<string> = ['array test1'],"
                                   + "@strArrayArg3 array<string> = ['a', 'b'],"
                                   + "@intArrayArg3 array<int32> = [1,2],"
                                   + "@intArg int32 = 10,"
                                   + "@doubleArg double = 10.1,"
                                   + "@booleanArg boolean = true,"
                                   + "@stringArg string = 'string',"
                                   + "@intMapArg map<int32> = {'key' : 1}," 
                                   + "@mapArg1 map<string> = {'key' : 'value'},"
                                   + "@mayMapArg map<map<string>> = {'mapKey': {'key' : 'value'}},"
                                   + "@mapArg2 map<string> = {'key1' : 'value1', 'key2':'value2'});" 
                                   + "SELECT @strArrayArg1, @strArrayArg2, @intArg, @doubleArg, @booleanArg, @mayMapArg OUTPUT AS program_arguments;";
        CompiledProgram program = compiler.compile(programStr);
        List<ArgumentInfo> argInfos = program.getArguments();
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("intArg", 12).put("doubleArg", 2.1).build(), true);
        List<Record> f1 = myResult.getResult("program_arguments").get().getResult();
        Assert.assertEquals(f1.get(0).get("strArrayArg1").toString(), "[array test1]");
        Assert.assertEquals(f1.get(0).get("intArg"), 12);
        Assert.assertEquals(f1.get(0).get("doubleArg"), 2.1);

        Map<String, ArgumentInfo> argMap = Maps.newHashMap();
        for (ArgumentInfo arguInfo:argInfos) {
          argMap.put(arguInfo.getName(), arguInfo);
        }
        assertEquals("[array test1]", argMap.get("strArrayArg1").getDefaultValue().toString());
        assertEquals("array test1", ((List)argMap.get("strArrayArg2").getDefaultValue()).get(0)); 
        assertEquals("value", ((Map)((Map)argMap.get("mayMapArg").getDefaultValue()).get("mapKey")).get("key"));
    }

    /*
     * Assert that empty value for key of type String is skipped (that is, filtered out by the container) if
     * so requested by the source (by declaring skipEmptyOrZero=true inside its @Key annotation)
     */
    @Test
    public void testEmptyStringKeyWithSkipEmptyOrZeroSetToTrue() throws Exception {
        /*
         * Assert empty string key is passed to source
         */
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id IN ('1', '', '3') OUTPUT AS out;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("out").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("", "", 0), new Person("3", "3", 3)));

        /*
         * Assert empty string key is filtered out by the container
         */
        injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleStringKeySourceWithSkipEmptyOrZeroSetToTrue.class));
        compiler = injector.getInstance(YQLPlusCompiler.class);
        program = compiler.compile("SELECT * FROM source WHERE id IN ('1', '', '3') OUTPUT AS out;");
        rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("out").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("3", "3", 3)));

        /*
         * Assert empty string key is filtered out by the container
         */
        injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleListOfStringKeySourceWithSkipEmptyOrZeroSetToTrue.class));
        compiler = injector.getInstance(YQLPlusCompiler.class);
        program = compiler.compile("SELECT * FROM source WHERE id IN ('1', '', '3') OUTPUT AS out;");
        rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("out").get().getResult(), ImmutableList.of(new Person("1", "1", 1), new Person("3", "3", 3)));
    }

    /*
     * Assert that zero value for key of type Integer is skipped (that is, filtered out by the container) if
     * so requested by the source (by declaring skipEmptyOrZero=true inside its @Key annotation)
     */
    @Test
    public void testZeroIntegerKeyWithSkipEmptyOrZeroSetToTrue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleIntegerKeySourceWithSkipEmptyOrZeroSetToTrue.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id IN (1, 0, 3) OUTPUT AS out;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("out").get().getResult(), ImmutableList.of(new IntegerPerson(Integer.valueOf(1), Integer.valueOf(1), 1),
                new IntegerPerson(Integer.valueOf(3), Integer.valueOf(3), 3)));
    }

    /*
     * Assert that zero value key of type Integer is not skipped (filtered out by the container) by default.
     */
    @Test
    public void testZeroIntegerKey() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", SingleIntegerKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM source WHERE id IN (1, 0, 3) OUTPUT AS out;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("out").get().getResult(), ImmutableList.of(new IntegerPerson(Integer.valueOf(1), Integer.valueOf(1), 1),
                new IntegerPerson(Integer.valueOf(0), Integer.valueOf(0), 0), new IntegerPerson(Integer.valueOf(3), Integer.valueOf(3), 3)));
    }

    /*
     * Key and source share the same name ("id"). Make sure that the key name is interpreted as a record field
     * instead of a record
     */
    @Test
    public void testSourceWithSameNameAsKey() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("some.id", SingleIntegerKeySource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM some.id WHERE id=4 OUTPUT AS out;");
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(rez.getResult("out").get().getResult(), ImmutableList.of(new IntegerPerson(Integer.valueOf(4), Integer.valueOf(4), 4)));
    }

    @Test
    public void testRecords() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("CREATE TEMPORARY TABLE tmpPeople AS (SELECT people.* FROM people); " +
                "SELECT m FROM tmpPeople m OUTPUT AS out;");
        ProgramResult programResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet yqlResultSet = programResult.getResult("out").get();
        List<Record> recordsList = yqlResultSet.getResult();
        Assert.assertEquals(recordsList.size(), 3);

        Record record = recordsList.get(0);
        record = (Record) record.get("m");
        // Assert record size is 6 (includes all the fields from table "people")
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);

        record = recordsList.get(1);
        record = (Record) record.get("m");
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "joe");
        Assert.assertEquals(record.get("id"), "2");
        Assert.assertEquals(record.get("iid"), 2);
        Assert.assertEquals(record.get("iidPrimitive"), 2);
        Assert.assertEquals(record.get("otherId"), "2");
        Assert.assertEquals(record.get("score"), 1);

        record = recordsList.get(2);
        record = (Record) record.get("m");
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "smith");
        Assert.assertEquals(record.get("id"), "3");
        Assert.assertEquals(record.get("iid"), 3);
        Assert.assertEquals(record.get("iidPrimitive"), 3);
        Assert.assertEquals(record.get("otherId"), "3");
        Assert.assertEquals(record.get("score"), 2);
    }

    @Test
    public void testListOfMapsRecord() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("listOfMapSource", ListOfMapSource.class,
                "stringUtilUDF", StringUtilUDF.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);

        CompiledProgram program = compiler.compile("PROGRAM (@tags array<string>); " +
                "SELECT * FROM listOfMapSource(\"tag\", @tags) OUTPUT AS out;");
        ProgramResult programResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("tags", Arrays.asList("tag1", "tag2", "tag3", "tag4")).build(), true);
        YQLResultSet yqlResultSet = programResult.getResult("out").get();
        List<Record> recordsList = yqlResultSet.getResult();
        assertEquals(4, recordsList.size());
        assertEquals("tag1", ((Map) recordsList.get(0)).get("tag"));
        assertEquals("tag2", ((Map) recordsList.get(1)).get("tag"));
        assertEquals("tag3", ((Map) recordsList.get(2)).get("tag"));
        assertEquals("tag4", ((Map) recordsList.get(3)).get("tag"));

        program = compiler.compile("PROGRAM (@tags array<string>); " +
                "FROM stringUtilUDF IMPORT toUpperCase; " +
                "SELECT toUpperCase(listOfMapSourceAlias[\"tag\"]) FROM listOfMapSource(\"tag\", @tags) listOfMapSourceAlias OUTPUT AS out;");
        programResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("tags", Arrays.asList("tag1", "tag2", "tag3", "tag4")).build(), true);
        yqlResultSet = programResult.getResult("out").get();
        recordsList = yqlResultSet.getResult();
        assertEquals(4, recordsList.size());
        assertEquals("TAG1", recordsList.get(0).get("expr"));
        assertEquals("TAG2", recordsList.get(1).get("expr"));
        assertEquals("TAG3", recordsList.get(2).get("expr"));
        assertEquals("TAG4", recordsList.get(3).get("expr"));
    }

    @Test
    public void testEmpytKeyList() throws Exception {
        String programStr = "PROGRAM (@category array<string>); \n" +
            "select id, category \n" +
            "from sampleListWithBoxedParams(10, 5.0, \"program\") \n" +
            "where category in (@category) order by id \n" +
            "output as sampleIds; \n";
        Injector injector = Guice.createInjector(new JavaTestModule(), new AbstractModule() {
            @Override
            protected void configure() {
                MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
                sourceBindings.addBinding("sampleListWithBoxedParams").toInstance(new SampleListSourceWithBoxedParams());
            }
        });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>().put("category", ImmutableList.of()).build(), true);
        YQLResultSet result = myResult.getResult("sampleIds").get();
        List<Record> resultSet = result.getResult();
        assertEquals(0, resultSet.size());
    }
    
    @Test
    public void testTempTableBehavior() throws Exception {
        PersonSource.resetIndex();
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("CREATE TEMPORARY TABLE tmpPeople AS (SELECT people.* FROM people); " +
                "SELECT m1 FROM tmpPeople m1 OUTPUT AS out1;" +
                "SELECT m2 FROM tmpPeople m2 OUTPUT AS out2;" +
                "SELECT m3 FROM tmpPeople m3 OUTPUT AS out3;");
        ProgramResult programResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> recordsList = programResult.getResult("out1").get().getResult();
        assertEquals(3, recordsList.size());
        recordsList = programResult.getResult("out2").get().getResult();
        assertEquals(3, recordsList.size());
        recordsList = programResult.getResult("out3").get().getResult();
        assertEquals(3, recordsList.size());
        assertEquals(1, PersonSource.getIndex());
    }
    
    @Test
    public void testArrayIndexAdapter() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule(
            "Collection", CollectionFunctionsUdf.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT id, value FROM people | Collection.asArray OUTPUT AS peoples;");
        ProgramResult programResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Record> findLike = programResult.getResult("peoples").get().getResult();
        assertEquals(3, findLike.size());
        assertEquals("1", findLike.get(0).get("id"));
        assertEquals("bob", findLike.get(0).get("value"));
        assertEquals("2", findLike.get(1).get("id"));
        assertEquals("joe", findLike.get(1).get("value"));
        assertEquals("3", findLike.get(2).get("id"));
        assertEquals("smith", findLike.get(2).get("value"));
    }
    
    @Test
    public void testLike() throws Exception {
        Pattern likePattern = Pattern.compile(".*joe.*");
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM people WHERE value LIKE '%joe%' OUTPUT AS peoples;");
        ProgramResult programResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Person> findLike = programResult.getResult("peoples").get().getResult();
        program = compiler.compile("SELECT * FROM people OUTPUT AS peoples;");
        List<Person> wholeList = program.run(ImmutableMap.<String, Object>of(), true).getResult("peoples").get().getResult();        
        List<Person> expectedResult = Lists.newArrayList();
        for (Person person:wholeList) {
            if (likePattern.matcher(person.getValue()).find()) {
                expectedResult.add(person);
            }
        }
        assertEquals(expectedResult, findLike);
    }
    
    @Test
    public void testMatch() throws Exception {
        Pattern likePattern = Pattern.compile(".*joe.*");
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM people WHERE value MATCHES '.*joe.*' OUTPUT AS peoples;");
        ProgramResult programResult = program.run(ImmutableMap.<String, Object>of(), true);
        List<Person> findMatch = programResult.getResult("peoples").get().getResult();
        program = compiler.compile("SELECT * FROM people OUTPUT AS peoples;");
        List<Person> wholeList = program.run(ImmutableMap.<String, Object>of(), true).getResult("peoples").get().getResult();        
        List<Person> expectedResult = Lists.newArrayList();
        for (Person person:wholeList) {
            if (likePattern.matcher(person.getValue()).find()) {
                expectedResult.add(person);
            }
        }
        assertEquals(expectedResult, findMatch);
    }
    
    @Test
    public void testKeyAndFieldNameCaseInSensitive() throws Exception {   
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("property.baseUrl", new BaseUrlMapSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String exptectedBaseUrl = "https://ca.celebrity.yaHoo.com/";
        String programStr = "SELECT  baseUrl FROM  property.baseUrl WHERE baseUrL = '" + exptectedBaseUrl + "' OUTPUT AS  baseurl_from_property_list;";
        List<Record> rez = compiler.compile(programStr).run(ImmutableMap.<String, Object>of(), true)
                                        .getResult("baseurl_from_property_list").get().getResult();
        assertEquals(exptectedBaseUrl, rez.get(0).get("BASEUrL"));
        
        programStr = "SELECT  baseURL FROM  property.baseUrl WHERE baseUrl = '" + exptectedBaseUrl + "' OUTPUT AS  baseurl_from_property_list;";
        rez = compiler.compile(programStr).run(ImmutableMap.<String, Object>of(), true)
            .getResult("baseurl_from_property_list").get().getResult();
        assertEquals(exptectedBaseUrl, rez.get(0).get("BaSEURL"));        
    }
    
    @Test
    public void testInsertCaseInSensitive() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new MovieSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (@uuid string, @title string, @category string, " +
                "@prodDate string, @duration int32, @reviews array<string>, @newRelease boolean, @rating byte);\n" +
                "INSERT INTO source (uuID, title, CATegory, prodDate, duration, reviews, newRelease, rating) " +
                "values (@uuid, @title, @category, @prodDate, @duration, @reviews, @newRelease, @rating) " +
                "OUTPUT AS out;");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .put("uuid", "1234").put("title", "Vertigo").put("category", "Mystery").put("prodDate", "1950")
                .put("duration", 120).put("reviews", reviews).put("newRelease", false).put("rating", (byte) 0x7a)
                .build(), true);
        List<Movie> result = myResult.getResult("out").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records inserted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
    }
    
    @Test
    public void testDeleteCaseInSensitive() throws Exception {
        MovieSource movieSource = new MovieSource();
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, ImmutableList.of("Great!", "Awesome!"),
                true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        List<String> reviews = ImmutableList.of("Great!", "Awesome!");
        movieSource.insertMovie("1234", "Vertigo", "Mystery", "1950", 120, reviews, true, (byte) 0x12, "Quentin Tarantino, Kate Winslet");
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", movieSource));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM ();\n" +
                "DELETE FROM source WHERE uuID = '1234' OUTPUT AS deleted;");
        ProgramResult myResult = program.run(null, true);

        // Missing RETURNING clause causes Class to be maintained
        List<Movie> result = myResult.getResult("deleted").get().getResult();
        Assert.assertEquals(result.size(), 1, "Wrong number of movie records deleted");

        Movie insertedMovie = result.get(0);
        Assert.assertEquals("1234", insertedMovie.getUuid());
        Assert.assertEquals("Vertigo", insertedMovie.getTitle());
        Assert.assertEquals("Mystery", insertedMovie.getCategory());
        Assert.assertEquals("1950", insertedMovie.getProdDate());
        Assert.assertEquals(120, insertedMovie.getDuration().intValue());
        Assert.assertEquals(reviews, insertedMovie.getReviews());
        Assert.assertTrue(insertedMovie.isNewRelease());
        Assert.assertEquals(insertedMovie.getRating(), (byte) 0x12);

        Assert.assertTrue(program.containsStatement(CompiledProgram.ProgramStatement.DELETE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.INSERT));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.UPDATE));
        Assert.assertFalse(program.containsStatement(CompiledProgram.ProgramStatement.SELECT));
    }
    
    @Test
    public void tesCollectionTypeAssignanle() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("samples", new SampleListSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM ();" + 
                            "CREATE TEMP TABLE tmp AS (SELECT * FROM samples); \n" +
                            "SELECT * FROM samples(@tmp) OUTPUT AS sampleList;";
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                    .build(), true);
        List<Sample> responses = myResult.getResult("sampleList").get().getResult();
        Assert.assertEquals(1, responses.size());
        Assert.assertTrue(responses.get(0) instanceof Sample);
    }
    
    @Test
    public void testWildcardTypeAdapt() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("BulkAddBalance", new StatusSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM ( @payload string);" + 
            "INSERT INTO BulkAddBalance (payload) VALUES (@payload)" +
            "OUTPUT AS bulkAddBalanceResponse;";
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                    .put("payload", "payload").build(), true);
        List<BulkResponse> responses = myResult.getResult("bulkAddBalanceResponse").get().getResult();
        Assert.assertEquals("500", responses.get(0).getBulkResponseItems().get(0).getErrorCode());
        Assert.assertEquals("id", responses.get(0).getBulkResponseItems().get(0).getId());
    }

    @Test
    public void testSelectWithoutSource() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = "PROGRAM(@next string=''); SELECT 2 WHERE @next='' OUTPUT AS out;";
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(new ImmutableMap.Builder<String, Object>()
                .build(), true);
        List obj = myResult.getResult("out").get().getResult();
        assertEquals(1, obj.size());
    }
    
    @Test
    public void testIntCompare() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new IntSource()),
            new AbstractModule() {
                @Override
                protected void configure() {
                    final MapBinder<String, Exports>
                    exportsBindings = MapBinder.newMapBinder(binder(), String.class, Exports.class);
                    exportsBindings.addBinding("BaseUDF").to(BaseUDF.class);
                }
        });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = 
                " CREATE TEMP TABLE aaa as ( " +
                "   SELECT 1 AS A1, 2 AS A2 "+
                " ); " +
                " CREATE TEMP TABLE bbb AS ( " +
                "   SELECT  c.id, BaseUDF.getWithDefault(a.A2, 0) as ddd " +
                "   FROM source c " +
                "   LEFT JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) " +
                " ); " +
                " SELECT * FROM bbb WHERE ddd = 0 OUTPUT AS out; ";
      
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet result = myResult.getResult("out").get();
        List<IntSource.SampleId> list = result.getResult();
        Assert.assertEquals(list.size(), 1);
    }
    
    @Test
    public void testSimpleLeftJoinOnCondition() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new IntSource())
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = 
                " CREATE TEMP TABLE aaa as ( " +
                "   SELECT 1 AS A1 "+
                " ); " +
                "   SELECT  * " +
                "   FROM source c " +
                "   LEFT JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (2,3) AND false AND true OUTPUT AS out;";
      
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(((List)myResult.getResult("out").get().getResult()).size(), 0);
    }
    
    @Test
    public void testLeftJoinOnCondition() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("source", new IntSource(), "stringUtilUDF", StringUtilUDF.class)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = 
                " CREATE TEMP TABLE aaa as ( " +
                "   SELECT 1 AS A1 "+
                " ); " +
                "   SELECT  * " +
                "   FROM source c " +
                "   LEFT JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) AND stringUtilUDF.compareStr('a', 'b')   OUTPUT AS out01;" +
                "   SELECT  * " +
                "   FROM source c " +
                "   LEFT JOIN aaa a on a.A1 = c.id " +
                "   WHERE stringUtilUDF.compareStr('a', 'b') AND c.id IN (1,2)   OUTPUT AS out02;" +
                "   SELECT  * " +
                "   FROM source c " +
                "   LEFT JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) AND false  OUTPUT AS out03;" +
                "   SELECT  * " +
                "   FROM source c " +
                "   LEFT JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) AND stringUtilUDF.compareStr('a', 'a')  OUTPUT AS out2;";
      
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(((List)myResult.getResult("out2").get().getResult()).size(), 2);
        Assert.assertEquals(((List)myResult.getResult("out01").get().getResult()).size(), 0);
        Assert.assertEquals(((List)myResult.getResult("out02").get().getResult()).size(), 0);
        Assert.assertEquals(((List)myResult.getResult("out03").get().getResult()).size(), 0);
    }
    
    @Test
    public void testJoinOnCondition() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                    new SourceBindingModule("source", new IntSource(), "stringUtilUDF", StringUtilUDF.class)
        );
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = 
                " CREATE TEMP TABLE aaa as ( " +
                "   SELECT 1 AS A1 "+
                " ); " +
                "   SELECT  * " +
                "   FROM source c " +
                "   JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) AND stringUtilUDF.compareStr('a', 'b')   OUTPUT AS out01;" +
                "   SELECT  * " +
                "   FROM source c " +
                "   JOIN aaa a on a.A1 = c.id " +
                "   WHERE stringUtilUDF.compareStr('a', 'b') AND c.id IN (1,2)   OUTPUT AS out02;" +
                "   SELECT  * " +
                "   FROM source c " +
                "   JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) AND false  OUTPUT AS out03;" +
                "   SELECT  * " +
                "   FROM source c " +
                "   JOIN aaa a on a.A1 = c.id " +
                "   WHERE c.id IN (1,2) AND stringUtilUDF.compareStr('a', 'a')  OUTPUT AS out2;";
      
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        Assert.assertEquals(((List)myResult.getResult("out2").get().getResult()).size(), 1);
        Assert.assertEquals(((List)myResult.getResult("out01").get().getResult()).size(), 0);
        Assert.assertEquals(((List)myResult.getResult("out02").get().getResult()).size(), 0);
        Assert.assertEquals(((List)myResult.getResult("out03").get().getResult()).size(), 0);
    }
    
    public static class BaseUDF implements Exports {
        @Export
        public static <T> T getWithDefault(final T input1, final T input2) {
            return input1 == null ? input2 : input1;
        }
    }
}
