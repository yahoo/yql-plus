/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.BindingNamespace;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ModuleType;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLPlusEngine;
import com.yahoo.yqlplus.engine.compiler.code.ASMClassSource;
import com.yahoo.yqlplus.engine.compiler.code.AnyTypeWidget;
import com.yahoo.yqlplus.engine.compiler.code.GambitScope;
import com.yahoo.yqlplus.engine.compiler.code.GambitSource;
import com.yahoo.yqlplus.engine.compiler.code.LambdaFactoryBuilder;
import com.yahoo.yqlplus.engine.compiler.code.NullExpr;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.compiler.runtime.ProgramInvocation;
import com.yahoo.yqlplus.engine.internal.generate.PhysicalExprOperatorCompiler;
import com.yahoo.yqlplus.engine.internal.plan.DynamicExpressionEnvironment;
import com.yahoo.yqlplus.engine.internal.plan.DynamicExpressionEvaluator;
import com.yahoo.yqlplus.engine.rules.LogicalProgramTransforms;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class CompilingTestBase {
    ASMClassSource source;
    GambitScope scope;
    protected BindingNamespace namespace;
    protected YQLPlusEngine.Builder builder;
    Map<String, OperatorNode<PhysicalExprOperator>> constants;

    protected Callable<Object> compileExpression(OperatorNode<PhysicalExprOperator> expr) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        LambdaFactoryBuilder callable = scope.createInvocableCallable();
        callable.addArgument("$program", scope.adapt(ProgramInvocation.class, false));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(callable);
        callable.complete(compiler.evaluateExpression(callable.local("$program"), new NullExpr(AnyTypeWidget.getInstance()), expr));
        scope.build();
        try {
            return (Callable<Object>) callable.getFactory().invokeWithArguments(new ProgramInvocation() {
                @Override
                public void readArguments(Map<String, Object> arguments) {

                }

                @Override
                protected void run() throws Exception {

                }
            });
        } catch (VerifyError e) {
            source.trace(System.err);
            throw e;
        } catch (InstantiationException e) {
            source.dump(System.err);
            throw e;
        } catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @BeforeMethod(groups={"init"})
    public void setUp() {
        this.source = new ASMClassSource();
        this.scope = new GambitSource(source);
        this.builder = YQLPlusEngine.builder();
        this.namespace = this.builder.binder();
        this.constants = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    }

    protected OperatorNode<PhysicalExprOperator> parseExpression(String expr) throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<ExpressionOperator> op = parser.parseExpression(expr, ImmutableSet.of(), ImmutableMap.of("map", ImmutableList.of("map")));
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
                String key = Joiner.on('.').join(path);
                if (constants.containsKey(key)) {
                    return constants.get(key);
                }
                if (path.size() < 2) {
                    throw new ProgramCompileException(location, "Module property reference expects at least two-argument path (module name, property name): %s", path);
                }
                ModuleType module = namespace.findModule(location, path.subList(0, path.size() - 1));
                if(module != null) {
                    String name = path.get(path.size() - 1);
                    return module.property(location, null, name);
                } else {
                    throw new ProgramCompileException(location, "Unknown module path %s", Joiner.on('.').join(path));
                }
            }

            @Override
            public OperatorNode<PhysicalExprOperator> constant(Object value) {
                return CompilingTestBase.this.constant(value);
            }
        };
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(env);
        return eval.apply(op);
    }

    protected void defineView(String name, String query) throws IOException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("program.yql", "CREATE VIEW " + name + " AS " + query + ";");
        Assert.assertEquals(program.getOperator(), StatementOperator.PROGRAM);
        for(OperatorNode<StatementOperator> statement : program.<List<OperatorNode<StatementOperator>>>getArgument(0)) {
            if(statement.getOperator() == StatementOperator.DEFINE_VIEW) {
                String viewName = statement.getArgument(0);
                OperatorNode<SequenceOperator> parsedQuery = statement.getArgument(1);
                namespace.bindView(viewName, parsedQuery);
            }
        }
    }

    protected void defineSource(String name, Class<? extends Source> sourceClazz) {
        namespace.bindSource(name, sourceClazz);
    }

    protected OperatorNode<StatementOperator> parseQueryProgram(String query) throws IOException {
        ProgramParser parser = new ProgramParser();
        LogicalProgramTransforms transforms = new LogicalProgramTransforms();
        OperatorNode<StatementOperator> program = parser.parse("program.yql", query + " OUTPUT AS f1;");
        program = transforms.apply(program, namespace);
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
        YQLPlusCompiler compiler = builder.build();
        CompiledProgram program = compiler.compile("program.yql", programText);
        return program.run(ImmutableMap.of());

    }

    protected <T> T runQueryProgram(String query) throws Exception {
        return runProgram(query + " OUTPUT AS f1;").getResult("f1").get().getResult();
    }

    protected CompiledProgram compileProgram(String programName, String program) throws Exception {
        YQLPlusCompiler compiler = builder.build();
        return compiler.compile(programName, program);
    }

    protected CompiledProgram compileProgramStream(String programName, InputStream stream) throws Exception {
        YQLPlusCompiler compiler = builder.build();
        return compiler.compile(programName, stream);
    }

    protected CompiledProgram compileProgramResource(String resourceName) throws Exception {
        return compileProgramStream(resourceName, getClass().getResourceAsStream(resourceName));
    }


    public OperatorNode<PhysicalExprOperator> constant(Object value) {
        return OperatorNode.create(PhysicalExprOperator.CONSTANT, source.getValueTypeAdapter().inferConstantType(value), value);
    }

    public OperatorNode<PhysicalExprOperator> constant(TypeWidget type, Object value) {
        return OperatorNode.create(PhysicalExprOperator.CONSTANT, type, value);
    }

    public void putConstant(String name, Object constant) {
        constants.put(name, constant(constant));
    }

    public void putExpr(String name, OperatorNode<PhysicalExprOperator> expr) {
        constants.put(name, expr);
    }

}
