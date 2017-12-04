/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Joiner;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import com.yahoo.cloud.metrics.api.DummyStandardRequestEmitter;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.RequestEvent;
import com.yahoo.cloud.metrics.api.RequestMetricSink;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.DummyTracer;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.api.NativeInvocationResultHandler;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.guice.EngineThreadPoolModule;
import com.yahoo.yqlplus.engine.guice.ExecutionScopeModule;
import com.yahoo.yqlplus.engine.guice.PhysicalOperatorBuiltinsModule;
import com.yahoo.yqlplus.engine.guice.PlannerCompilerModule;
import com.yahoo.yqlplus.engine.guice.ProgramTracerModule;
import com.yahoo.yqlplus.engine.guice.SearchNamespaceModule;
import com.yahoo.yqlplus.engine.guice.SourceApiModule;
import com.yahoo.yqlplus.engine.internal.bytecode.exprs.NullExpr;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.CallableInvocableBuilder;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitScope;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitSource;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ObjectBuilder;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.PhysicalExprOperatorCompiler;
import com.yahoo.yqlplus.engine.internal.generate.NativeSerialization;
import com.yahoo.yqlplus.engine.internal.generate.ProgramInvocation;
import com.yahoo.yqlplus.engine.internal.java.runtime.RelativeTicker;
import com.yahoo.yqlplus.engine.internal.java.runtime.TimeoutTracker;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.DynamicExpressionEnvironment;
import com.yahoo.yqlplus.engine.internal.plan.DynamicExpressionEvaluator;
import com.yahoo.yqlplus.engine.internal.plan.ModuleNamespace;
import com.yahoo.yqlplus.engine.internal.plan.ModuleType;
import com.yahoo.yqlplus.engine.internal.plan.SourceNamespace;
import com.yahoo.yqlplus.engine.internal.plan.SourceType;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.ConditionalsBuiltinsModule;
import com.yahoo.yqlplus.engine.internal.plan.streams.SequenceBuiltinsModule;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.AnyTypeWidget;
import com.yahoo.yqlplus.engine.internal.source.SourceUnitGenerator;
import com.yahoo.yqlplus.engine.java.JavaTestModule;
import com.yahoo.yqlplus.engine.rules.LogicalProgramTransforms;
import com.yahoo.yqlplus.engine.scope.EmptyExecutionScope;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CompilingTestBase implements ViewRegistry, SourceNamespace, ModuleNamespace {
    Injector injector;
    ASMClassSource source;
    GambitScope scope;
    Map<String, OperatorNode<SequenceOperator>> views;
    Map<String, OperatorNode<PhysicalExprOperator>> modules;
    Map<String, Provider<? extends Source>> sources;
    JavaTestModule.MetricModule metricModule;


    public class CompilingTestModule extends AbstractModule {
        @Override
        protected void configure() {
            install(new EngineThreadPoolModule());
            install(new ExecutionScopeModule());
            install(new PlannerCompilerModule());
            install(new ProgramTracerModule());
            install(new SearchNamespaceModule());
            install(new SourceApiModule());
            install(new PhysicalOperatorBuiltinsModule());
            install(metricModule);
            Multibinder<SourceNamespace> sourceNamespaceMultibinder = Multibinder.newSetBinder(binder(), SourceNamespace.class);
            Multibinder<ModuleNamespace> moduleNamespaceMultibinder = Multibinder.newSetBinder(binder(), ModuleNamespace.class);
            sourceNamespaceMultibinder.addBinding().toInstance(CompilingTestBase.this);
            moduleNamespaceMultibinder.addBinding().toInstance(CompilingTestBase.this);
            bind(ViewRegistry.class).toInstance(CompilingTestBase.this);
        }
    }

    @BeforeMethod
    public void setUp() {
        init();
    }

    public void init(Module... modules) {
        if(modules == null) {
            modules = new Module[0];
        }
        this.metricModule = new JavaTestModule.MetricModule();
        injector = Guice.createInjector(Iterables.concat(ImmutableList.<Module>of(new CompilingTestModule()), Arrays.asList(modules)));
        source = injector.getInstance(ASMClassSource.class);
        scope = new GambitSource(source);
        this.modules = Maps.newLinkedHashMap();
        this.views = Maps.newHashMap();
        this.sources = Maps.newLinkedHashMap();
    }

    @Override
    public ModuleType findModule(Location location, ContextPlanner planner, List<String> modulePath) {
        if(ImmutableList.of("yql", "sequences").equals(modulePath)) {
            return new SequenceBuiltinsModule();
        } else if(ImmutableList.of("yql", "conditionals").equals(modulePath)) {
            return new ConditionalsBuiltinsModule();
        }
        return null;
    }

    @Override
    public SourceType findSource(Location location, ContextPlanner planner, List<String> path) {
        String pathKey = Joiner.on('.').join(path);
        Provider<? extends Source> source = sources.get(pathKey);
        if (source == null) {
            return null;
        } else {
            SourceUnitGenerator adapter = new SourceUnitGenerator(planner.getGambitScope());
            return adapter.apply(path, source);
        }
    }

    protected Callable<Object> compileExpression(OperatorNode<PhysicalExprOperator> expr) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        CallableInvocableBuilder callable = scope.createInvocableCallable();
        callable.addArgument("$program", scope.adapt(ProgramInvocation.class, false));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(callable);
        callable.complete(compiler.evaluateExpression(callable.local("$program"), new NullExpr(AnyTypeWidget.getInstance()), expr));
        scope.build();
        ObjectBuilder builder = callable.builder();
        Class<Callable<Object>> clazz = (Class<Callable<Object>>) scope.getObjectClass(builder);

        try {
            return clazz.getConstructor(ProgramInvocation.class).newInstance(new ProgramInvocation() {
                @Override
                public void readArguments(Map<String, Object> arguments) {

                }

                @Override
                public void run() {

                }

                @Override
                protected NativeSerialization getNativeSerializer() {
                    throw new UnsupportedOperationException();
                }
            });
        } catch (VerifyError e) {
            source.trace(System.err);
            throw e;
        } catch (InstantiationException e) {
            source.dump(System.err);
            throw e;
        }
    }

    protected OperatorNode<PhysicalExprOperator> parseExpression(String expr) throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<ExpressionOperator> op = parser.parseExpression(expr, ImmutableSet.<String>of(), ImmutableMap.<String, List<String>>of("map", ImmutableList.of("map")));
        DynamicExpressionEnvironment env = new DynamicExpressionEnvironment() {
            @Override
            public OperatorValue getVariable(String name) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OperatorNode<PhysicalExprOperator> call(OperatorNode<ExpressionOperator> call) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OperatorNode<PhysicalExprOperator> call(OperatorNode<ExpressionOperator> call, OperatorNode<PhysicalExprOperator> row) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OperatorNode<PhysicalExprOperator> evaluate(OperatorNode<ExpressionOperator> value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OperatorNode<PhysicalExprOperator> property(Location location, List<String> path) {
                return modules.get(Joiner.on('.').join(path.subList(1, path.size())));
            }

            @Override
            public OperatorNode<PhysicalExprOperator> constant(Object value) {
                return CompilingTestBase.this.constant(value);
            }
        };
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(env);
        return eval.apply(op);
    }

    @Override
    public OperatorNode<SequenceOperator> getView(List<String> name) {
        return views.get(Joiner.on(".").join(name));
    }

    protected void defineView(String name, String query) throws IOException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("program.yql", "CREATE VIEW " + name + " AS " + query + ";");
        Assert.assertEquals(program.getOperator(), StatementOperator.PROGRAM);
        for(OperatorNode<StatementOperator> statement : program.<List<OperatorNode<StatementOperator>>>getArgument(0)) {
            if(statement.getOperator() == StatementOperator.DEFINE_VIEW) {
                String viewName = statement.getArgument(0);
                OperatorNode<SequenceOperator> parsedQuery = statement.getArgument(1);
                views.put(viewName, parsedQuery);
            }
        }
    }

    protected void defineSource(String name, Provider<Source> sourceProvider) {
        sources.put(name, sourceProvider);
    }

    protected void defineSource(String name, Class<? extends Source> sourceClazz) {
        sources.put(name, (Provider<? extends Source>)injector.getProvider(sourceClazz));
    }

    protected OperatorNode<StatementOperator> parseQueryProgram(String query) throws IOException {
        ProgramParser parser = new ProgramParser();
        LogicalProgramTransforms transforms = new LogicalProgramTransforms();
        OperatorNode<StatementOperator> program = parser.parse("program.yql", query + " OUTPUT AS f1;");
        program = transforms.apply(program, this);
        Assert.assertEquals(program.getOperator(), StatementOperator.PROGRAM);
        return program;
    }

    protected OperatorNode<SequenceOperator> parseQuery(String query) throws IOException, RecognitionException {
        OperatorNode<StatementOperator> program = parseQueryProgram(query);
        List<OperatorNode<StatementOperator>> statements = program.getArgument(0);
        for(OperatorNode<StatementOperator> statement : statements) {
            if(statement.getOperator() == StatementOperator.EXECUTE) {
                if(statement.getArgument(1).equals("f1")) {
                    return statement.getArgument(0);
                }
            }
        }
        throw new IllegalStateException("Unable to find f1 output query in parsed program?: " + program);
    }

    protected ProgramResult runProgram(String programText) throws Exception {
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("program.yql", programText);
        return program.run(ImmutableMap.<String,Object>of(), true);

    }

    protected <T> T runQueryProgram(String query) throws Exception {
        return runProgram(query + " OUTPUT AS f1;").getResult("f1").get().getResult();
    }

    protected CompiledProgram compileProgram(String programName, String program) throws Exception {
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        return compiler.compile(programName, program);
    }

    protected CompiledProgram compileProgramStream(String programName, InputStream stream) throws Exception {
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        return compiler.compile(programName, stream);
    }

    protected CompiledProgram compileProgramResource(String resourceName) throws Exception {
        return compileProgramStream(resourceName, getClass().getResourceAsStream(resourceName));
    }


    protected ByteArrayOutputStream runQueryProgramSerialized(NativeEncoding encoding, String query, Module... modules) throws Exception {
        if(modules != null && modules.length > 0) {
            init(modules);
        }
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("program.yql", query + " OUTPUT AS f1;");
        final CompletableFuture<ByteArrayOutputStream> result = new CompletableFuture<>();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        program.invoke(encoding, new NativeInvocationResultHandler() {

            @Override
            public void fail(Throwable t) {
                result.completeExceptionally(t);
            }

            @Override
            public OutputStream createStream(String name) {
                if("f1".equals(name)) {
                    return outputStream;
                } else {
                    return new ByteArrayOutputStream();
                }
            }

            @Override
            public void succeed(String name) {
                if("f1".equals(name)) {
                    result.complete(outputStream);
                }
            }

            @Override
            public void fail(String name, Throwable t) {
                if("f1".equals(name)) {
                    result.completeExceptionally(t);
                }
            }

            @Override
            public void end() {

            }
        }, ImmutableMap.<String,Object>of(), new EmptyExecutionScope(), new TaskContext(new DummyStandardRequestEmitter(new MetricDimension(), new RequestMetricSink() {
            @Override
            public void emitRequest(RequestEvent requestEvent) {

            }
        }), new DummyTracer(), new TimeoutTracker(30L, TimeUnit.SECONDS, new RelativeTicker(Ticker.systemTicker()))));
        return result.get(30L, TimeUnit.SECONDS);
    }

    public OperatorNode<PhysicalExprOperator> constant(Object value) {
        return OperatorNode.create(PhysicalExprOperator.CONSTANT, source.getValueTypeAdapter().inferConstantType(value), value);
    }

    public OperatorNode<PhysicalExprOperator> constant(TypeWidget type, Object value) {
        return OperatorNode.create(PhysicalExprOperator.CONSTANT, type, value);
    }

    public void putConstant(String name, Object constant) {
        modules.put(name, constant(constant));
    }

    public void putExpr(String name, OperatorNode<PhysicalExprOperator> expr) {
        modules.put(name, expr);
    }

}
