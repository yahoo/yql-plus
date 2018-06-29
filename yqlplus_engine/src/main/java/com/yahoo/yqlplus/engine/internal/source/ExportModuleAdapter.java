/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.generate.ObjectBuilder;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.DynamicExpressionEvaluator;
import com.yahoo.yqlplus.engine.internal.plan.ModuleType;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ExportModuleAdapter implements ModuleType {
    private final TypeWidget type;
    private final String moduleName;
    private final Multimap<String, ObjectBuilder.MethodBuilder> methods;
    private final Map<String, ObjectBuilder.MethodBuilder> fields;

    private OperatorNode<PhysicalExprOperator> module;

    public ExportModuleAdapter(TypeWidget type, String moduleName, Multimap<String, ObjectBuilder.MethodBuilder> methods, Map<String, ObjectBuilder.MethodBuilder> fields) {
        this.type = type;
        this.moduleName = moduleName;
        this.methods = methods;
        this.fields = fields;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> call(Location location, ContextPlanner context, String name, List<OperatorNode<ExpressionOperator>> arguments) {
        return callInRowContext(location, context, name, arguments, null);
    }

    @Override
    public OperatorNode<PhysicalExprOperator> callInRowContext(Location location, ContextPlanner context, String name, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row) {
        Collection<ObjectBuilder.MethodBuilder> targets = methods.get(name);
        if (targets.isEmpty()) {
            throw new ProgramCompileException(location, "Method '%s' not found on module %s", name, moduleName);
        }
        @SuppressWarnings("ConstantConditions") GambitCreator.Invocable firstInvocable = Iterables.getFirst(targets, null).invoker();
        TypeWidget outputType = firstInvocable.getReturnType();
        DynamicExpressionEvaluator eval = row == null ? new DynamicExpressionEvaluator(context) : new DynamicExpressionEvaluator(context, row);
        List<OperatorNode<PhysicalExprOperator>> callArgs = Lists.newArrayList();
        callArgs.add(getModule(location, context));
        callArgs.add(context.getContextExpr());
        if (targets.size() > 1) {
            callArgs.addAll(eval.applyAll(arguments));
            for (ObjectBuilder.MethodBuilder candidate : targets) {
                outputType = context.getValueTypeAdapter().unifyTypes(outputType, candidate.invoker().getReturnType());
            }
            return OperatorNode.create(location, PhysicalExprOperator.CALL, outputType, name, callArgs);
        } else {
            Iterator<TypeWidget> args = firstInvocable.getArgumentTypes().iterator();
            args.next();
            args.next();
            // consume the module & context
            Iterator<OperatorNode<ExpressionOperator>> e = arguments.iterator();
            while (args.hasNext()) {
                if (!e.hasNext()) {
                    throw new ProgramCompileException(location, "Argument length mismatch in call to %s (expects %d arguments)", name, firstInvocable.getArgumentTypes().size());
                }
                callArgs.add(OperatorNode.create(location, PhysicalExprOperator.CAST, args.next(), eval.apply(e.next())));
            }
            if (e.hasNext()) {
                throw new ProgramCompileException(location, "Argument length mismatch in call to %s (expects %d arguments)", name, firstInvocable.getArgumentTypes().size());
            }
            return OperatorNode.create(location, PhysicalExprOperator.INVOKE, firstInvocable, callArgs);
        }
    }

    private OperatorNode<PhysicalExprOperator> getModule(Location location, ContextPlanner planner) {
        if (module == null) {
            OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                    OperatorNode.create(location, PhysicalExprOperator.ROOT_CONTEXT),
                    OperatorNode.create(location, PhysicalExprOperator.INJECT_MEMBERS,
                            OperatorNode.create(location, PhysicalExprOperator.NEW,
                                    type,
                                    ImmutableList.of())));
            module = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
        }
        return module;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> property(Location location, ContextPlanner context, String name) {
        ObjectBuilder.MethodBuilder fieldGetter = fields.get(name);
        if (fieldGetter == null) {
            throw new ProgramCompileException(location, "Property '%s' not found on module %s", name, moduleName);
        }
        return OperatorNode.create(location, PhysicalExprOperator.INVOKE, fieldGetter.invoker(), ImmutableList.of(getModule(location, context)));
    }

    @Override
    public StreamValue pipe(Location location, ContextPlanner context, String name, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments) {
        Collection<ObjectBuilder.MethodBuilder> targets = methods.get(name);
        if (targets.isEmpty()) {
            throw new ProgramCompileException(location, "Method '%s' not found on module %s", name, moduleName);
        }
        @SuppressWarnings("ConstantConditions") GambitCreator.Invocable firstInvocable = Iterables.getFirst(targets, null).invoker();
        TypeWidget outputType = firstInvocable.getReturnType();
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(context);
        List<OperatorNode<PhysicalExprOperator>> callArgs = Lists.newArrayList();
        callArgs.add(getModule(location, context));
        callArgs.add(context.getContextExpr());
        if (targets.size() > 1) {
            callArgs.add(input.materializeValue());
            callArgs.addAll(eval.applyAll(arguments));
            for (ObjectBuilder.MethodBuilder candidate : targets) {
                outputType = context.getValueTypeAdapter().unifyTypes(outputType, candidate.invoker().getReturnType());
            }
            return StreamValue.iterate(context, OperatorNode.create(location, PhysicalExprOperator.CALL, outputType, name, callArgs));
        } else {
            Iterator<TypeWidget> args = firstInvocable.getArgumentTypes().iterator();
            args.next();
            args.next();
            // consume the module & context
            callArgs.add(input.materializeValue());
            // consume the stream argument
            args.next();
            Iterator<OperatorNode<ExpressionOperator>> e = arguments.iterator();
            while (args.hasNext()) {
                if (!e.hasNext()) {
                    throw new ProgramCompileException(location, "Argument length mismatch in call to %s (expects %d arguments)", name, firstInvocable.getArgumentTypes().size());
                }
                callArgs.add(OperatorNode.create(location, PhysicalExprOperator.CAST, args.next(), eval.apply(e.next())));
            }
            if (e.hasNext()) {
                throw new ProgramCompileException(location, "Argument length mismatch in call to %s (expects %d arguments)", name, firstInvocable.getArgumentTypes().size());
            }
            return StreamValue.iterate(context, OperatorNode.create(location, PhysicalExprOperator.INVOKE, firstInvocable, callArgs));
        }
    }
}
