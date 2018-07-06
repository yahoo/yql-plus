/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.guice.JavaEngineModule;
import com.yahoo.yqlplus.engine.sources.NullIterableSource;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Test
public class JavaEngineTest {
    static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();

    public static class Spec {
        private Map<String, List<String>> vars;
        private List<TestEntry> queries;

        public Map<String, List<String>> getVars() {
            return vars;
        }

        public void setVars(Map<String, List<String>> vars) {
            this.vars = vars;
        }

        public List<TestEntry> getQueries() {
            return queries;
        }

        public void setQueries(List<TestEntry> queries) {
            this.queries = queries;
        }


        private void generate(List<List<String>> output, List<List<String>> sources) {
            ArrayList<String> combination = Lists.newArrayListWithCapacity(sources.size());
            generate(0, output, combination, sources);
        }

        private void generate(int s, List<List<String>> output, List<String> combination, List<List<String>> sources) {
            if (s == sources.size()) {
                output.add(ImmutableList.copyOf(combination));
            } else {
                List<String> source = sources.get(s);
                final int last = combination.size();
                for (String item : source) {
                    combination.add(item);
                    generate(s + 1, output, combination, sources);
                    combination.remove(last);
                }
            }
        }

        public List<TestEntry> compute() throws IOException {
            List<TestEntry> results = Lists.newArrayList();
            // for each var, try every combination of other vars
            // avoid repeating combinations

            Map<String, Integer> vars = Maps.newHashMap();
            List<List<String>> vals = Lists.newArrayListWithCapacity(this.vars.size());
            for (Map.Entry<String, List<String>> e : this.vars.entrySet()) {
                vars.put(e.getKey(), vars.size());
                vals.add(e.getValue());
            }
            // assumes the var names do not need escaping for regex
            Pattern pattern = Pattern.compile("\\{\\{((?:" + Joiner.on(")|(").join(vars.keySet()) + "))\\}\\}", Pattern.CASE_INSENSITIVE);
            List<List<String>> combinations = Lists.newArrayList();
            generate(combinations, vals);
            StringBuffer out = new StringBuffer();
            Set<String> seen = Sets.newHashSet();
            for (List<String> combination : combinations) {
                for (TestEntry entry : queries) {
                    Matcher m = pattern.matcher(entry.getInput());
                    String q = replaceVars(vars, out, combination, m);
                    if (seen.add(q)) {
                        JsonNode output = entry.getOutput();
                        // if the output value is a string, substitute it and turn it into a tree
                        if (output.isTextual()) {
                            String rez = replaceVars(vars, out, combination, pattern.matcher(output.textValue()));
                            output = JSON_FACTORY.createParser(rez).readValueAsTree();
                        }
                        results.add(new TestEntry(q, output));
                    }
                }
            }
            return results;
        }

        private String replaceVars(Map<String, Integer> vars, StringBuffer out, List<String> combination, Matcher m) {
            out.setLength(0);
            while (m.find()) {
                String name = m.group(1);
                m.appendReplacement(out, combination.get(vars.get(name)));
            }
            m.appendTail(out);
            return out.toString();
        }
    }

    public static class TestEntry {
        public String input;
        public JsonNode output;

        public TestEntry() {
        }

        public TestEntry(String input, JsonNode output) {
            this.input = input;
            this.output = output;
        }

        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        public JsonNode getOutput() {
            return output;
        }

        public void setOutput(JsonNode output) {
            this.output = output;
        }
    }

    public static class FailureSource implements Source {
        private final ScheduledExecutorService toyWorker;

        @Inject
        FailureSource(@Named("toy") ScheduledExecutorService toyWorker) {
            this.toyWorker = toyWorker;
        }

        @Query
        public Iterable<Person> scan() {
            throw new RuntimeException("I do not work at all!");
        }

        @Query
        public ListenableFuture<Iterable<Person>> futureScan(int delay) {
            return eventually(delay, new Callable<Iterable<Person>>() {
                @Override
                public Iterable<Person> call() {
                    return scan();
                }
            });
        }

        private <V> ListenableFuture<V> eventually(long delay, final Callable<V> todo) {
            final SettableFuture<V> result = SettableFuture.create();
            toyWorker.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.set(todo.call());
                    } catch (Exception e) {
                        result.setException(e);
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
            return result;
        }


    }

    protected List<TestEntry> parseTestTree(InputStream inputStream) throws IOException {
        JsonParser parser = JSON_FACTORY.createParser(inputStream);
        Spec spec = parser.readValueAs(Spec.class);
        return spec.compute();
    }


    protected Object[][] loadParseTrees(String resourceName) throws IOException {
        List<TestEntry> entries = parseTestTree(getClass().getResourceAsStream(resourceName));
        Object[][] result = new Object[entries.size()][];
        int i = -1;
        for (TestEntry entry : entries) {
            result[++i] = new Object[]{entry.input, entry.output};
        }
        return result;
    }

    protected com.google.inject.Module[] createModules() {
        return new com.google.inject.Module[]{
                new SourceBindingModule(
                        "person", ToyPersonSource.class,
                        "failuresource", FailureSource.class
                ),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ScheduledExecutorService.class).annotatedWith(Names.named("toy")).toInstance(
                                Executors.newSingleThreadScheduledExecutor()
                        );
                        bind(ViewRegistry.class).toInstance(new ViewRegistry() {
                            @Override
                            public OperatorNode<SequenceOperator> getView(List<String> name) {
                                return null;
                            }
                        });
                    }
                }
        };
    }

    @DataProvider(name = "queries", parallel = true)
    public Object[][] loadParseTrees() throws IOException {
        return loadParseTrees("javaqueries.json");
    }

    @Test(dataProvider = "queries")
    public void testQueries(String input, JsonNode outputTree) throws Exception {
        runParseTree(input, outputTree);
    }

    @Test
    public void requireProblematicQuery() throws Exception {
        run("SELECT value FROM person(100) WHERE id IN ('100', '101', '102') LIMIT 1 OFFSET 1 OUTPUT AS f1;", createModules());
    }

    protected void runParseTree(String input, JsonNode expectedOutput) throws Exception {
        Map<String, JsonNode> result = run(input, createModules());
        Set<String> have = Sets.newTreeSet(result.keySet());
        Iterator<String> fields = expectedOutput.fieldNames();
        Set<String> expect = Sets.newTreeSet();
        while (fields.hasNext()) {
            expect.add(fields.next());
        }
        Assert.assertEquals(have, expect);
        for (String key : have) {
            Assert.assertEquals(result.get(key), expectedOutput.get(key));
        }
    }

    protected Map<String, JsonNode> run(String script, final com.google.inject.Module... modules) throws Exception {
        Injector injector = Guice.createInjector(new JavaEngineModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        for (com.google.inject.Module module : modules) {
                            install(module);
                        }
                    }
                });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(script);
        //program.dump(System.err);
        ProgramResult result = program.run(Maps.newHashMap());
        try {
            result.getEnd().get(10000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Map<String, JsonNode> parsed = Maps.newLinkedHashMap();
        for (String key : result.getResultNames()) {
            YQLResultSet data = result.getResult(key).get();
            Object rez = data.getResult();
            ByteArrayOutputStream outstream = new ByteArrayOutputStream();
            JsonGenerator gen = JSON_FACTORY.createGenerator(outstream);
            gen.writeObject(rez);
            gen.flush();
            parsed.put(key, JSON_FACTORY.createParser(outstream.toByteArray()).readValueAsTree());
        }
        return parsed;
    }

    /**
     * Unit test for Ticket 6999179 (NULL value returned from the list data source causes future timeout )
     *
     * @throws Exception
     */
    @Test
    public void testNullListCausesTimeout() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(), new SourceBindingModule("source", new NullIterableSource()));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("PROGRAM (); \n" +
                "SELECT * FROM source WHERE id IN (1, 2, 3) " +
                "OUTPUT AS out;");
        ProgramResult myResult = program.run(ImmutableMap.of());
        /*
         * Without the fix, the following line would throw:
         * java.util.concurrent.ExecutionException: java.util.concurrent.TimeoutException: Timeout after ... NANOSECONDS.
         */
        List<NullIterableSource.SampleId> list = myResult.getResult("out").get().getResult();
        Assert.assertEquals(list.size(), 0);
    }
}
