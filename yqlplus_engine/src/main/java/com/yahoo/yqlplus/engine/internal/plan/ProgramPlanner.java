/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.types.*;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.api.DependencyNotFoundException;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitScope;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.AnyTypeWidget;
import com.yahoo.yqlplus.engine.internal.tasks.Value;
import com.yahoo.yqlplus.engine.rules.LogicalProgramTransforms;
import com.yahoo.yqlplus.engine.rules.LogicalTransforms;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.logical.TypeOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Exploring a new approach -- make a second pass to plan against still-abstract but closer-to-real "physical" operators.
 * <p/>
 * Rewrite entire program into a series of single-output tasks.
 * <p/>
 * Rewrite THAT into a parallelizable tree of closures.
 * <p/>
 * Make a pass noting which values are passed between closures.
 * <p/>
 * Model sequences as values being written into an "accumulator" which can then be materialized.
 * <p/>
 * Inputs:
 * Program
 * Available data source planners
 */
public class ProgramPlanner implements ViewRegistry {
    private final LogicalProgramTransforms programTransforms = new LogicalProgramTransforms();
    private final LogicalTransforms logicalTransforms;
    private final Map<String, OperatorNode<SequenceOperator>> views;
    private final ConstantExpressionEvaluator constantExpressionEvaluator;
    private final SourceNamespace sourceNamespace;
    private final ModuleNamespace moduleNamespace;
    private final Map<String, OperatorValue> variables = Maps.newHashMap();
    private final List<Value> terminals = Lists.newArrayList();
    private final ContextPlanner rootContext;
    private final AtomicBoolean once = new AtomicBoolean(false);
    private final Map<Object, OperatorNode<PhysicalExprOperator>> valueConstants = Maps.newHashMap();
    private final GambitScope adapter;
    private final EnumSet<CompiledProgram.ProgramStatement> writeStatements = EnumSet.noneOf(CompiledProgram.ProgramStatement.class);
    private final ViewRegistry parentViews;

    private final Map<String, SourceType> resolvedSources = Maps.newHashMap();
    private final Map<String, ModuleType> resolvedModules = Maps.newHashMap();

    public ProgramPlanner(LogicalTransforms transforms, SourceNamespace sourceNamespace, ModuleNamespace moduleNamespace, GambitScope gambitScope, ViewRegistry viewNamespace) {
        this.logicalTransforms = transforms;
        this.sourceNamespace = sourceNamespace;
        this.moduleNamespace = moduleNamespace;
        this.rootContext = new ContextPlanner(this);
        this.constantExpressionEvaluator = new ConstantExpressionEvaluator();
        this.views = Maps.newHashMap();
        this.adapter = gambitScope;
        this.parentViews = viewNamespace;
    }

    public SourceType findSource(ContextPlanner contextPlanner, OperatorNode<SequenceOperator> source) {
        List<String> path = source.getArgument(0);
        String name = Joiner.on(".").join(path);
        if (resolvedSources.containsKey(name)) {
            return resolvedSources.get(name);
        }
        SourceType result = sourceNamespace.findSource(source.getLocation(), contextPlanner, path);
        if (result == null) {
            throw new DependencyNotFoundException(source.getLocation(), "Source '%s' not found", name);
        }
        resolvedSources.put(name, result);
        return result;
    }

    public ModuleType findModule(Location location, ContextPlanner contextPlanner, List<String> path) {
        String name = Joiner.on(".").join(path);
        if (resolvedModules.containsKey(name)) {
            return resolvedModules.get(name);
        }
        ModuleType result = moduleNamespace.findModule(location, contextPlanner, path);
        if (result == null) {
            throw new DependencyNotFoundException(location, "Module '%s' not found", name);
        }
        resolvedModules.put(name, result);
        return result;
    }

    public StreamValue pipe(Location location, ContextPlanner context, StreamValue input, List<String> path, List<OperatorNode<ExpressionOperator>> arguments) {
        if (path.size() < 2) {
            throw new ProgramCompileException(location, "PIPE expects at least two-argument path (module name, function name): %s", path);
        }
        ModuleType module = findModule(location, context, path.subList(0, path.size() - 1));
        String name = path.get(path.size() - 1);
        return module.pipe(location, context, name, input, arguments);
    }

    public OperatorNode<PhysicalExprOperator> call(ContextPlanner context, OperatorNode<ExpressionOperator> call) {
        return call(context, call, null);
    }

    public OperatorNode<PhysicalExprOperator> call(ContextPlanner context, OperatorNode<ExpressionOperator> call, OperatorNode<PhysicalExprOperator> row) {
        Preconditions.checkArgument(call.getOperator() == ExpressionOperator.CALL);
        List<String> path = call.getArgument(0);
        if (path.size() < 2) {
            throw new ProgramCompileException(call.getLocation(), "CALL expects at least two-argument path (module name, function name): %s", call);
        }
        ModuleType module = findModule(call.getLocation(), context, path.subList(0, path.size() - 1));
        String name = path.get(path.size() - 1);
        List<OperatorNode<ExpressionOperator>> arguments = call.getArgument(1);
        if (row != null) {
            return module.callInRowContext(call.getLocation(), context, name, arguments, row);
        } else {
            return module.call(call.getLocation(), context, name, arguments);
        }
    }

    public OperatorNode<PhysicalExprOperator> property(ContextPlanner context, Location location, List<String> path) {
        if (path.size() < 2) {
            throw new ProgramCompileException(location, "Module property reference expects at least two-argument path (module name, property name): %s", path);
        }
        ModuleType module = findModule(location, context, path.subList(0, path.size() - 1));
        String name = path.get(path.size() - 1);
        return module.property(location, context, name);
    }


    public OperatorNode<TaskOperator> plan(String programName, InputStream program) throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> parsedProgram = parser.parse(programName, program);
        return plan(parsedProgram);
    }

    public OperatorNode<TaskOperator> plan(String programName, String program) throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> parsedProgram = parser.parse(programName, program);
        return plan(parsedProgram);
    }

    public OperatorNode<TaskOperator> plan(OperatorNode<StatementOperator> program) throws IOException {
        Preconditions.checkArgument(program.getOperator() == StatementOperator.PROGRAM);
        if (!once.compareAndSet(false, true)) {
            throw new ProgramCompileException("ProgramPlanners cannot be reused");
        }
        program = programTransforms.apply(program, this);
        List<OperatorNode<StatementOperator>> statements = program.getArgument(0);

        // TODO: VIEWs cannot not be permitted to reference arguments
        // TODO: VIEW processing will be fixed in a future commit

        List<OperatorNode<TaskOperator>> arguments = Lists.newArrayList();

        for (OperatorNode<StatementOperator> stmt : statements) {
            switch (stmt.getOperator()) {
                case ARGUMENT: {
                    // ARGUMENT name typeName defaultValue (nullable)
                    // we ignore typeNames for now
                    String name = stmt.getArgument(0);
                    OperatorNode<TypeOperator> typeNode = stmt.getArgument(1);
                    YQLType type = toYQLType(typeNode);
                    OperatorNode<ExpressionOperator> defaultValue = stmt.getArgument(2);
                    OperatorValue arg;
                    if (defaultValue.getOperator() != ExpressionOperator.NULL) {
                        Object value = constantExpressionEvaluator.apply(defaultValue);
                        Preconditions.checkArgument(value != null, "Argument default constant-evaluated as null: " + defaultValue);
                        arg = OperatorStep.create(getValueTypeAdapter(), stmt.getLocation(), PhysicalOperator.OPTIONAL_ARGUMENT, name, type, value);
                        type = YQLOptionalType.create(type);
                    } else {
                        type = YQLOptionalType.deoptional(type);
                        arg = OperatorStep.create(getValueTypeAdapter(), stmt.getLocation(), PhysicalOperator.REQUIRED_ARGUMENT, name, type);
                    }
                    define(name, arg);
                    arguments.add(OperatorNode.create(stmt.getLocation(), TaskOperator.ARGUMENT, name, type));
                    break;
                }
                case DEFINE_VIEW:
                    defineView((String) stmt.getArgument(0), logicalTransforms.apply((OperatorNode<SequenceOperator>) stmt.getArgument(1), this));
                    break;
                case EXECUTE: {
                    OperatorNode<SequenceOperator> query = stmt.getArgument(0);
                    String variableName = stmt.getArgument(1);
                    OperatorNode<SequenceOperator> transformedQuery = logicalTransforms.apply(query, this);
                    ContextPlanner planner = rootContext.create(variableName);
                    OperatorValue out = planner.execute(transformedQuery).materialize();
                    // TODO: context end needs to be tied more intimately with value resolution
                    // we have two scenarios for contexts:
                    //   1) it's created and ended in a single scope (including attaching to an async output)
                    //   2) it's created and ended as associated with a stream (merge)
                    define(variableName, planner.end(out));
                    break;
                }
                case OUTPUT: {
                    String name = stmt.getArgument(0);
                    addTerminal(OperatorStep.create(getValueTypeAdapter(), stmt.getLocation(), PhysicalOperator.OUTPUT, name, OperatorNode.create(PhysicalExprOperator.VALUE, getVariable(name))));
                    break;
                }
                case COUNT: {
                    String name = stmt.getArgument(0);
                    addTerminal(OperatorStep.create(getValueTypeAdapter(), stmt.getLocation(), PhysicalOperator.OUTPUT, name, OperatorNode.create(PhysicalExprOperator.LENGTH, OperatorNode.create(PhysicalExprOperator.VALUE, getVariable(name)))));
                    break;
                }
                default:
                    throw new ProgramCompileException("Unknown StatementOperator type: " + stmt.getOperator());
            }
        }
        OperatorValue end = OperatorStep.create(getValueTypeAdapter(), PhysicalOperator.END, terminals);
        PlanTree tree = new PlanTree();
        OperatorNode<TaskOperator> x = tree.planTask(arguments, end.getSource());
        x.putAnnotation("yql.writeStatements", writeStatements);
        return x;
    }

    public void addTerminal(Value value) {
        terminals.add(value);
    }


    private void define(String name, OperatorValue value) {
        value.setName(name);
        variables.put(name, value);
    }

    String define(OperatorValue value) {
        String name = "sym_" + variables.size();
        value.setName(name);
        variables.put(name, value);
        return name;
    }

    public void defineView(String name, OperatorNode<SequenceOperator> query) {
        if (views.containsKey(name)) {
            throw new ProgramCompileException(query.getLocation(), "View '%s' already defined", name);
        }
        views.put(name, query);
    }

    public OperatorValue getVariable(String name) {
        return variables.get(name);
    }

    public OperatorNode<PhysicalExprOperator> constant(Object value) {
        if(value == null) {
            return OperatorNode.create(PhysicalExprOperator.NULL, AnyTypeWidget.getInstance());
        }
        if (valueConstants.containsKey(value)) {
            return valueConstants.get(value);
        }
        TypeWidget constantType = adapter.getValueTypeAdapter().inferConstantType(value);
        OperatorNode<PhysicalExprOperator> vexpr = OperatorNode.create(PhysicalExprOperator.CONSTANT, constantType, value);
        valueConstants.put(value, vexpr);
        return vexpr;
    }

    @Override
    public OperatorNode<SequenceOperator> getView(List<String> name) {
        if (name.size() == 1 && views.containsKey(name.get(0))) {
            return views.get(name.get(0));
        }
        return parentViews.getView(name);
    }

    private YQLType toYQLType(OperatorNode<TypeOperator> typeNode) {
        switch (typeNode.getOperator()) {
            case BYTE:
                return YQLBaseType.INT8;
            case INT16:
                return YQLBaseType.INT16;
            case INT32:
                return YQLBaseType.INT32;
            case INT64:
                return YQLBaseType.INT64;
            case STRING:
                return YQLBaseType.STRING;
            case DOUBLE:
                return YQLBaseType.FLOAT64;
            case TIMESTAMP:
                return YQLBaseType.TIMESTAMP;
            case BOOLEAN:
                return YQLBaseType.BOOLEAN;
            case ARRAY: {
                return YQLArrayType.create(toYQLType((OperatorNode<TypeOperator>) typeNode.getArgument(0)));
            }
            case MAP: {
                return YQLMapType.create(YQLBaseType.STRING, toYQLType((OperatorNode<TypeOperator>) typeNode.getArgument(0)));
            }
        }
        throw new ProgramCompileException(typeNode.getLocation(), "Unknown TypeOperator %s", typeNode.getOperator());
    }

    public ProgramValueTypeAdapter getValueTypeAdapter() {
        return adapter.getValueTypeAdapter();
    }

    public GambitScope getGambitScope() {
        return adapter;
    }

    public void addStatement(CompiledProgram.ProgramStatement writeStatementType) {
        writeStatements.add(writeStatementType);
    }
}
