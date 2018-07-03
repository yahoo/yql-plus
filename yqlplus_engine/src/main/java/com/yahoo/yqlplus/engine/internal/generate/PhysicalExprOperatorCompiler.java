/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.yqlplus.compiler.code.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.code.ArrayTypeWidget;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.compiler.code.BaseTypeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeCastExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.ExactInvocation;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.code.GambitTypes;
import com.yahoo.yqlplus.compiler.code.InvocableBuilder;
import com.yahoo.yqlplus.compiler.code.IterableTypeWidget;
import com.yahoo.yqlplus.compiler.code.IterateAdapter;
import com.yahoo.yqlplus.compiler.code.LambdaFactoryBuilder;
import com.yahoo.yqlplus.compiler.code.LambdaInvocable;
import com.yahoo.yqlplus.compiler.code.ListTypeWidget;
import com.yahoo.yqlplus.compiler.code.MapTypeWidget;
import com.yahoo.yqlplus.compiler.code.NotNullableTypeWidget;
import com.yahoo.yqlplus.compiler.code.NullTestedExpression;
import com.yahoo.yqlplus.compiler.code.NullableTypeWidget;
import com.yahoo.yqlplus.compiler.code.ObjectBuilder;
import com.yahoo.yqlplus.compiler.code.PropertyAdapter;
import com.yahoo.yqlplus.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.runtime.ArithmeticOperation;
import com.yahoo.yqlplus.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.compiler.runtime.KeyAccumulator;
import com.yahoo.yqlplus.compiler.runtime.RecordAccumulator;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalProjectOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.SinkOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.StreamOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class PhysicalExprOperatorCompiler {
    public static final MetricDimension EMPTY_DIMENSION = new MetricDimension();
    private ScopedBuilder scope;

    public PhysicalExprOperatorCompiler(ScopedBuilder scope) {
        this.scope = scope;
    }


    public BytecodeExpression evaluateExpression(final BytecodeExpression program, final BytecodeExpression context, final OperatorNode<PhysicalExprOperator> expr) {
        switch (expr.getOperator()) {
            case ASYNC_INVOKE:
            case INVOKE: {
                GambitCreator.Invocable invocable = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(1);
                List<BytecodeExpression> arguments = evaluateExpressions(program, context, args);
                BytecodeExpression result = scope.invoke(expr.getLocation(), invocable, arguments);
                if (expr.getOperator() == PhysicalExprOperator.ASYNC_INVOKE) {
                    return scope.resolve(expr.getLocation(), getTimeout(context, expr.getLocation()), result);
                }
                return result;
            }
            case CALL: {
                TypeWidget outputType = expr.getArgument(0);
                String name = expr.getArgument(1);
                List<OperatorNode<PhysicalExprOperator>> arguments = expr.getArgument(2);
                List<BytecodeExpression> argumentExprs = evaluateExpressions(program, context, arguments);
                return scope.call(expr.getLocation(), outputType, name, argumentExprs);
            }
            case CONSTANT: {
                TypeWidget t = expr.getArgument(0);
                Object cval = expr.getArgument(1);
                return scope.constant(t, cval);
            }
            case ROOT_CONTEXT: {
                return scope.propertyValue(expr.getLocation(), program, "rootContext");
            }
            case CURRENT_CONTEXT:
                return context;
            case TRACE_CONTEXT: {
                OperatorNode<PhysicalExprOperator> attrs = expr.getArgument(0);
                final BytecodeExpression metricDimension = asMetricDimension(program, context, attrs);
                return scope.invokeExact(expr.getLocation(), "start", TaskContext.class, context.getType(), context, metricDimension);
            }
            case TIMEOUT_MAX: {
                final BytecodeExpression timeout = scope.cast(BaseTypeAdapter.INT64, evaluateExpression(program, context, expr.getArgument(0)));
                final BytecodeExpression units = evaluateExpression(program, context, expr.getArgument(1));
                return scope.invokeExact(expr.getLocation(), "timeout", TaskContext.class, context.getType(), context, timeout, units);
            }
            case TIMEOUT_GUARD: {
                final BytecodeExpression min = scope.cast(BaseTypeAdapter.INT64, evaluateExpression(program, context, expr.getArgument(0)));
                final BytecodeExpression minUnits = evaluateExpression(program, context, expr.getArgument(1));
                final BytecodeExpression max = scope.cast(BaseTypeAdapter.INT64, evaluateExpression(program, context, expr.getArgument(2)));
                final BytecodeExpression maxUnits = evaluateExpression(program, context, expr.getArgument(3));
                return scope.invokeExact(expr.getLocation(), "timeout", TaskContext.class, context.getType(), context, min, minUnits, max, maxUnits);
            }
            case END_CONTEXT: {
                final BytecodeExpression output = evaluateExpression(program, context, expr.getArgument(0));
                GambitCreator.ScopeBuilder child = scope.scope();
                BytecodeExpression result = child.evaluateInto(output);
                child.exec(child.invokeExact(expr.getLocation(), "end", TaskContext.class, BaseTypeAdapter.VOID, context));
                return child.complete(result);
            }
            case VALUE: {
                OperatorValue value = expr.getArgument(0);
                return resolveValue(expr.getLocation(), program, context, value);
            }
            case CAST: {
                final TypeWidget outputType = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> input = expr.getArgument(1);
                final BytecodeExpression output = evaluateExpression(program, context, input);
                return scope.cast(expr.getLocation(), outputType, output);
            }
            case FOREACH: {
                BytecodeExpression inputExpr = evaluateExpression(program, context, expr.getArgument(0));
                OperatorNode<FunctionOperator> function = expr.getArgument(1);
                TypeWidget itemType = inputExpr.getType().getIterableAdapter().getValue();
                GambitCreator.Invocable functionImpl = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType), function);
                return scope.transform(expr.getLocation(), inputExpr, functionImpl.prefix(program, context));
            }
            case FIRST: {
                BytecodeExpression inputExpr = evaluateExpression(program, context, expr.getArgument(0));
                return scope.first(expr.getLocation(), inputExpr);
            }
            case SINGLETON: {
                OperatorNode<PhysicalExprOperator> value = expr.getArgument(0);
                BytecodeExpression targetExpression = evaluateExpression(program, context, value);
                return scope.invokeExact(expr.getLocation(), "singleton", ProgramInvocation.class, new ListTypeWidget(targetExpression.getType()), program, scope.cast(AnyTypeWidget.getInstance(), targetExpression));
            }
            case LENGTH: {
                BytecodeExpression inputExpr = evaluateExpression(program, context, expr.getArgument(0));
                return scope.length(expr.getLocation(), inputExpr);
            }
            case PROPREF: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                String propertyName = expr.getArgument(1);
                BytecodeExpression targetExpr = evaluateExpression(program, context, target);
                return scope.propertyValue(expr.getLocation(), targetExpr, propertyName);
            }
            case INDEX: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> index = expr.getArgument(1);
                BytecodeExpression targetExpr = evaluateExpression(program, context, target);
                BytecodeExpression indexExpr = evaluateExpression(program, context, index);
                return scope.indexValue(expr.getLocation(), targetExpr, indexExpr);
            }
            case WITH_CONTEXT: {
                GambitCreator.ScopeBuilder contextScope = scope.scope();
                PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(contextScope);
                BytecodeExpression ctxExpr = contextScope.evaluateInto(compiler.evaluateExpression(program, context, expr.getArgument(0)));
                OperatorNode<PhysicalExprOperator> exprTarget = expr.getArgument(1);
                BytecodeExpression resultExpr = compiler.evaluateExpression(program, ctxExpr, OperatorNode.create(PhysicalExprOperator.END_CONTEXT, exprTarget));
                return contextScope.complete(resultExpr);
            }
            case CONCAT: {
                List<OperatorNode<PhysicalExprOperator>> iterables = expr.getArgument(0);
                List<BytecodeExpression> exprs = evaluateExpressions(program, context, iterables);
                List<TypeWidget> types = Lists.newArrayList();
                for (BytecodeExpression e : exprs) {
                    types.add(e.getType().getIterableAdapter().getValue());
                }

                return scope.cast(expr.getLocation(), new IterableTypeWidget(scope.unify(types)),
                        ExactInvocation.boundInvoke(Opcodes.INVOKESTATIC, "concat", scope.adapt(Iterables.class, false),
                                scope.adapt(Iterable.class, false),
                                scope.cast(scope.adapt(Iterable.class, false),
                                        scope.list(expr.getLocation(), exprs))).invoke(expr.getLocation()));
            }
            case ENFORCE_TIMEOUT: {
                List<BytecodeExpression> localExprs = Lists.newArrayList();
                List<TypeWidget> types = Lists.newArrayList();
                List<String> localNames = expr.getOperator().localsFor(expr);
                localExprs.add(program);
                localExprs.add(context);
                for (String local : localNames) {
                    BytecodeExpression arg = scope.local(expr.getLocation(), local);
                    localExprs.add(arg);
                    types.add(arg.getType());
                }
                LambdaInvocable invocation = compileSupplier(program.getType(), context.getType(), types,
                        OperatorNode.create(FunctionOperator.FUNCTION, localNames, expr.getArgument(0)));
                BytecodeExpression supplier = invocation.invoke(expr.getLocation(), program, context);
                GambitCreator.Invocable timeoutInvocation = scope.findExactInvoker(TaskContext.class, "runTimeout", AnyTypeWidget.getInstance(), Supplier.class);
                return scope.cast(invocation.getResultType(), timeoutInvocation.invoke(expr.getLocation(), context, supplier));
            }
            case LOCAL: {
                String localName = expr.getArgument(0);
                return scope.local(expr.getLocation(), localName);
            }
            case EQ:
            case NEQ: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return expr.getOperator() == PhysicalExprOperator.EQ ? scope.eq(expr.getLocation(), leftExpr, rightExpr)
                        : scope.neq(expr.getLocation(), leftExpr, rightExpr);
            }
            case BOOLEAN_COMPARE: {
                final BinaryComparison booleanComparison = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(1);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(2);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return scope.compare(expr.getLocation(), booleanComparison, leftExpr, rightExpr);
            }

            case BINARY_MATH: {
                final ArithmeticOperation arithmeticOperation = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(1);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(2);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return scope.arithmetic(expr.getLocation(), arithmeticOperation, leftExpr, rightExpr);
            }
            case COMPARE: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return scope.compare(expr.getLocation(), leftExpr, rightExpr);
            }
            case MULTICOMPARE: {
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(0);
                List<BytecodeExpression> expressions = evaluateExpressions(program, context, exprs);
                return scope.composeCompare(expressions);
            }
            case COALESCE: {
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(0);
                List<BytecodeExpression> expressions = evaluateExpressions(program, context, exprs);
                return scope.coalesce(expr.getLocation(), expressions);
            }
            case IF: {
                OperatorNode<PhysicalExprOperator> test = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> ifTrue = expr.getArgument(1);
                OperatorNode<PhysicalExprOperator> ifFalse = expr.getArgument(2);
                return handleIfTail(program, context, scope.createCase(), test, ifTrue, ifFalse);
            }
            case STREAM_EXECUTE: {
                OperatorNode<PhysicalExprOperator> input = expr.getArgument(0);
                OperatorNode<StreamOperator> stream = expr.getArgument(1);
                return streamExecute(program, context, input, stream);
            }
            case STREAM_CREATE: {
                OperatorNode<StreamOperator> t = expr.getArgument(0);
                return compileStreamCreate(program, context, t);
            }
            case STREAM_COMPLETE: {
                OperatorNode<PhysicalExprOperator> streamExpression = expr.getArgument(0);
                BytecodeExpression streamExpr = evaluateExpression(program, context, streamExpression);
                return scope.invoke(expr.getLocation(),
                        ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "complete", streamExpr.getType(),
                                // ideally this would unify the types of the input streams
                                new ListTypeWidget(AnyTypeWidget.getInstance()), streamExpr));
            }
            case RECORD: {
                List<String> names = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(1);
                List<BytecodeExpression> evaluated = evaluateExpressions(program, context, exprs);
                GambitCreator.RecordBuilder recordBuilder = scope.record();
                for (int i = 0; i < names.size(); ++i) {
                    recordBuilder.add(expr.getLocation(), names.get(i), evaluated.get(i));
                }
                return recordBuilder.build();
            }
            case RECORD_AS: {
                TypeWidget recordType = expr.getArgument(0);
                List<String> names = expr.getArgument(1);
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(2);
                List<BytecodeExpression> evaluated = evaluateExpressions(program, context, exprs);
                Map<String, BytecodeExpression> sets = Maps.newLinkedHashMap();
                for (int i = 0; i < names.size(); ++i) {
                    sets.put(names.get(i), evaluated.get(i));
                }
                if (!recordType.hasProperties()) {
                    throw new ProgramCompileException(expr.getLocation(), "Type passed to RECORD_AS has no properties", recordType.getTypeName());
                }
                return recordType.getPropertyAdapter().construct(sets);
            }
            case PROJECT: {
                List<OperatorNode<PhysicalProjectOperator>> operations = expr.getArgument(0);
                GambitCreator.RecordBuilder recordBuilder;
                if("map".equals(expr.getAnnotation("project:type"))) {
                    recordBuilder = scope.dynamicRecord();
                } else {
                    recordBuilder = scope.record();
                }
                for(OperatorNode<PhysicalProjectOperator> op : operations) {
                    switch(op.getOperator()) {
                        case FIELD: {
                            OperatorNode<PhysicalExprOperator> fieldValue = op.getArgument(0);
                            String fieldName = op.getArgument(1);
                            recordBuilder.add(expr.getLocation(), fieldName, evaluateExpression(program, context, fieldValue));
                            break;
                        }
                        case MERGE: {
                            OperatorNode<PhysicalExprOperator> fieldValue = op.getArgument(0);
                            recordBuilder.merge(expr.getLocation(), evaluateExpression(program, context, fieldValue));
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException("Unknown PhysicalProjectOperator: " + op.getOperator());
                    }
                }
                return recordBuilder.build();

            }
            case NULL: {
                TypeWidget type = expr.getArgument(0);
                return scope.nullFor(type);
            }
            case GENERATE_KEYS: {
                List<String> names = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> valueLists = expr.getArgument(1);
                List<BytecodeExpression> insns = Lists.newArrayListWithCapacity(names.size() * 2);
                for (int i = 0; i < names.size(); ++i) {
                    insns.add(scope.constant(names.get(i)));
                    BytecodeExpression keyExpr = evaluateExpression(program, context, valueLists.get(i));
                    insns.add(scope.cast(AnyTypeWidget.getInstance(), keyExpr));
                }

                BytecodeExpression arr = scope.array(expr.getLocation(), BaseTypeAdapter.ANY, insns);
                GambitCreator.Invocable factory = scope.constructor(keyCursorFor(scope, names), arr.getType());
                return scope.invoke(expr.getLocation(), factory, arr);
            }
            case OR: {
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(0);
                return scope.or(expr.getLocation(), evaluateExpressions(program, context, args));
            }
            case AND: {
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(0);
                return scope.and(expr.getLocation(), evaluateExpressions(program, context, args));
            }
            case IN: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                return scope.in(expr.getLocation(),
                        evaluateExpression(program, context, left),
                        evaluateExpression(program, context, right));
            }
            case CONTAINS: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                return scope.contains(expr.getLocation(),
                        evaluateExpression(program, context, left),
                        evaluateExpression(program, context, right));
            }
            case MATCHES: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                return scope.matches(expr.getLocation(),
                        evaluateExpression(program, context, left),
                        evaluateExpression(program, context, right));
            }
            case NOT: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.not(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case NEGATE: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.negate(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case IS_NULL: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.isNull(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case BOOL: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.bool(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case ARRAY: {
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(0);
                return scope.list(expr.getLocation(), evaluateExpressions(program, context, args));
            }
            case CATCH: {
                OperatorNode<PhysicalExprOperator> primary = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> fallback = expr.getArgument(1);
                return scope.fallback(expr.getLocation(),
                        evaluateExpression(program, context, primary),
                        evaluateExpression(program, context, fallback));
            }
            case NEW: {
                TypeWidget type = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> valueLists = expr.getArgument(1);
                List<BytecodeExpression> exprs = evaluateExpressions(program, context, valueLists);
                List<TypeWidget> types = Lists.newArrayList();
                for (BytecodeExpression e : exprs) {
                    types.add(e.getType());
                }
                return scope.invoke(expr.getLocation(), scope.constructor(type, types), exprs);
            }
            case INJECT_MEMBERS: {
                OperatorNode<PhysicalExprOperator> value = expr.getArgument(0);
                final BytecodeExpression result = evaluateExpression(program, context, value);
                final BytecodeExpression injector = getInjector(program, context);
                return new BaseTypeExpression(result.getType()) {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.exec(result);
                        code.dup(result.getType());
                        code.exec(injector);
                        code.swap(injector.getType(), result.getType());
                        code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Injector.class),
                                "injectMembers", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Object.class)), true);
                    }
                };


            }
            default:
                throw new ProgramCompileException("Unimplemented PhysicalExprOperator: " + expr.toString());
        }
    }

    private BytecodeExpression handleIfTail(BytecodeExpression program, BytecodeExpression context, GambitCreator.CaseBuilder caseBuilder, OperatorNode<PhysicalExprOperator> test, OperatorNode<PhysicalExprOperator> ifTrue, OperatorNode<PhysicalExprOperator> ifFalse) {
        caseBuilder.when(evaluateExpression(program, context, test), evaluateExpression(program, context, ifTrue));
        if(ifFalse.getOperator() == PhysicalExprOperator.IF) {
            OperatorNode<PhysicalExprOperator> nextTest = ifFalse.getArgument(0);
            OperatorNode<PhysicalExprOperator> nextTruth = ifFalse.getArgument(1);
            OperatorNode<PhysicalExprOperator> nextFalse = ifFalse.getArgument(2);
            return handleIfTail(program, context, caseBuilder, nextTest, nextTruth, nextFalse);
        }
        return caseBuilder.exit(evaluateExpression(program, context, ifFalse));
    }


    private BytecodeExpression getTimeout(BytecodeExpression context, Location loc) {
        return scope.propertyValue(loc, context, "timeout");
    }

    private BytecodeExpression compileStreamCreate(BytecodeExpression program, BytecodeExpression context, OperatorNode<StreamOperator> node) {
        ObjectBuilder stream = scope.createObject(RecordAccumulator.class);
        stream.addParameter("$program", program.getType());
        stream.addParameter("$context", context.getType());
        StreamSink pipeline = new SkipNullsSink(compileStream(program, context, node));
        ObjectBuilder.MethodBuilder finish = stream.method("finish");
        BytecodeExpression input = finish.addArgument("input", finish.list(AnyTypeWidget.getInstance()).getType());
        GambitCreator.ScopeBuilder scope = finish.scope();
        pipeline.prepare(scope, scope.local("$program"), scope.local("$context"), AnyTypeWidget.getInstance());
        GambitCreator.IterateBuilder iterate = scope.iterate(input);
        pipeline.item(iterate, iterate.getItem());
        finish.exit(pipeline.end(scope, iterate));
        return scope.invoke(node.getLocation(), scope.constructor(stream.type(), program.getType(), context.getType()).prefix(program, context));
    }

    public BytecodeExpression resolveValue(Location loc, BytecodeExpression program, BytecodeExpression ctx, OperatorValue value) {
        String name = value.getName();
        if (name == null) {
            throw new ProgramCompileException("Unnamed OperatorValue");
        }
        BytecodeExpression valueExpr = scope.propertyValue(loc, program, name);
        return scope.resolve(loc, getTimeout(ctx, loc), valueExpr);
    }

    private GambitCreator.Invocable compileFunction(TypeWidget programType, TypeWidget contextType, List<TypeWidget> argumentTypes, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        InvocableBuilder out = this.scope.createInvocable();
        out.addArgument("$program", programType);
        out.addArgument("$context", contextType);
        for (int i = 0; i < argumentNames.size(); ++i) {
            out.addArgument(argumentNames.get(i), argumentTypes.get(i));
        }
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(out);
        BytecodeExpression result = compiler.evaluateExpression(out.local("$program"), out.local("$context"), functionBody);
        return out.complete(result);
    }

    private LambdaInvocable compileSupplier(TypeWidget programType, TypeWidget contextType, List<TypeWidget> argumentTypes, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        LambdaFactoryBuilder builder = this.scope.createLambdaBuilder(Supplier.class, "get", Object.class, true);
        builder.addArgument("$program", programType);
        builder.addArgument("$context", contextType);
        for (int i = 0; i < argumentNames.size(); ++i) {
            builder.addArgument(argumentNames.get(i), argumentTypes.get(i));
        }
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(builder);
        BytecodeExpression result = compiler.evaluateExpression(builder.local("$program"), builder.local("$context"), functionBody);
        return builder.complete(result);
    }

    private LambdaInvocable compileComparator(TypeWidget programType, TypeWidget contextType, TypeWidget itemType, OperatorNode<FunctionOperator> function) {
        // TODO Comparator.class - int compare(Object left, Object right);
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);

        LambdaFactoryBuilder builder = this.scope.createLambdaBuilder(Comparator.class, "compare", int.class, false, Object.class, Object.class);
        BytecodeExpression programExpr = builder.addArgument("$program", programType);
        BytecodeExpression contextExpr = builder.addArgument("$context", contextType);
        BytecodeExpression leftExpr = builder.addArgument("left", AnyTypeWidget.getInstance());
        BytecodeExpression rightExpr = builder.addArgument("right", AnyTypeWidget.getInstance());
        builder.evaluateInto(argumentNames.get(0),
                builder.cast(function.getLocation(), itemType, leftExpr));
        builder.evaluateInto(argumentNames.get(1),
                builder.cast(function.getLocation(), itemType, rightExpr));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(builder);
        BytecodeExpression result = compiler.evaluateExpression(programExpr, contextExpr, functionBody);
        return builder.complete(result);
    }


    private BytecodeExpression asMetricDimension(BytecodeExpression program, BytecodeExpression context, OperatorNode<PhysicalExprOperator> attrs) {
        List<String> keys = attrs.getArgument(0);
        List<OperatorNode<PhysicalExprOperator>> vals = attrs.getArgument(1);
        MetricDimension dims = EMPTY_DIMENSION;
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            OperatorNode<PhysicalExprOperator> expr = vals.get(i);
            if (expr.getOperator() == PhysicalExprOperator.CONSTANT) {
                String val = expr.getArgument(1).toString();
                dims = dims.with(key, val);
            } else {
                return dynamicMetricDimension(program, context, attrs);
            }
        }
        return scope.constant(dims);
    }

    private BytecodeExpression dynamicMetricDimension(BytecodeExpression program, BytecodeExpression context, OperatorNode<PhysicalExprOperator> attrs) {
        List<String> keys = attrs.getArgument(0);
        List<OperatorNode<PhysicalExprOperator>> vals = attrs.getArgument(1);
        final List<BytecodeExpression> valueExprs = evaluateExpressions(program, context, vals);
        BytecodeExpression metric = scope.constant(EMPTY_DIMENSION);
        for (int i = 0; i < keys.size(); ++i) {
            BytecodeExpression key = scope.constant(keys.get(i));
            BytecodeExpression value = scope.cast(vals.get(i).getLocation(), BaseTypeAdapter.STRING, valueExprs.get(i));
            metric = scope.invokeExact(attrs.getLocation(), "with", MetricDimension.class, scope.adapt(MetricDimension.class, false), metric, key, value);
        }
        return metric;
    }

    private List<BytecodeExpression> evaluateExpressions(BytecodeExpression program, BytecodeExpression context, List<OperatorNode<PhysicalExprOperator>> vals) {
        List<BytecodeExpression> output = Lists.newArrayListWithExpectedSize(vals.size());
        for (OperatorNode<PhysicalExprOperator> expr : vals) {
            output.add(evaluateExpression(program, context, expr));
        }
        return output;
    }

    private BytecodeExpression getInjector(BytecodeExpression program, BytecodeExpression context) {
        return ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "getInjector", scope.adapt(ProgramInvocation.class, false), scope.adapt(Injector.class, false), program).invoke(Location.NONE);
    }

    private BytecodeExpression streamExecute(final BytecodeExpression program, final BytecodeExpression ctxExpr, OperatorNode<PhysicalExprOperator> input, OperatorNode<StreamOperator> stream) {
        BytecodeExpression streamInput = evaluateExpression(program, ctxExpr, input);
        StreamSink streamPipeline = new SkipNullsSink(compileStream(program, ctxExpr, stream));
        GambitCreator.ScopeBuilder scope = this.scope.scope();
        final BytecodeExpression timeout = getTimeout(ctxExpr, input.getLocation());
        streamInput = scope.resolve(input.getLocation(), timeout, streamInput);
        Preconditions.checkArgument(streamInput.getType().isIterable(), "streamExecute argument must be iterable");
        GambitCreator.IterateBuilder iterateBuilder = scope.iterate(streamInput);
        streamPipeline.prepare(scope, program, ctxExpr, iterateBuilder.getItem().getType());
        streamPipeline.item(iterateBuilder, iterateBuilder.getItem());
        return streamPipeline.end(scope, iterateBuilder);
    }

    public TypeWidget keyCursorFor(GambitTypes types, List<String> names) {
        ObjectBuilder builder = types.createObject(KeyAccumulator.class);
        ObjectBuilder.ConstructorBuilder bld = builder.getConstructor();
        bld.invokeSpecial(KeyAccumulator.class, bld.addArgument("$lst", new ArrayTypeWidget(Type.getType("[Ljava/lang/Object;"), AnyTypeWidget.getInstance())));
        ObjectBuilder.MethodBuilder adapter = builder.method("createKey");
        //     protected abstract RECORD createKey(List<KEY> columns);
        BytecodeExpression lst = adapter.addArgument("$list", NotNullableTypeWidget.create(new ListTypeWidget(AnyTypeWidget.getInstance())));
        TypeWidget keyType = BaseTypeAdapter.STRUCT;
        PropertyAdapter propertyAdapter = keyType.getPropertyAdapter();
        Map<String, BytecodeExpression> makeFields = Maps.newLinkedHashMap();
        int i = 0;
        List<TypeWidget> keyTypes = Lists.newArrayList();
        for (String name : names) {
            TypeWidget propertyType = propertyAdapter.getPropertyType(name);
            keyTypes.add(propertyType);
            makeFields.put(name, adapter.cast(Location.NONE, propertyType, adapter.indexValue(Location.NONE, lst, adapter.constant(i++))));
        }
        adapter.exit(adapter.cast(Location.NONE, types.adapt(Record.class, false), propertyAdapter.construct(makeFields)));
        builder.setTypeWidget(new KeyCursorTypeWidget(builder.getJVMType(), names, keyType));
        return builder.type();
    }


    interface StreamSink {
        void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType);

        void item(GambitCreator.IterateBuilder loop, BytecodeExpression item);

        BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop);
    }

    abstract class BaseStreamSink implements StreamSink {
        protected BytecodeExpression program;
        protected BytecodeExpression ctxExpr;

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            this.program = program;
            this.ctxExpr = context;
        }
    }

    abstract class BaseTransformSink extends BaseStreamSink {
        private final StreamSink next;

        protected BaseTransformSink(StreamSink next) {
            this.next = next;
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            super.prepare(scope, program, context, itemType);
            next.prepare(scope, program, context, itemType);
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            next.item(loop, item);
        }

        @Override
        public BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            return next.end(scope, loop);
        }
    }

    abstract class AccumulatedTransformSink extends BaseStreamSink {
        private final StreamSink next;
        private final AccumulatingSink accumulatingSink;

        protected AccumulatedTransformSink(StreamSink next) {
            this.next = next;
            this.accumulatingSink = new AccumulatingSink();
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            super.prepare(scope, program, context, itemType);
            this.accumulatingSink.prepare(scope, program, context, itemType);
        }

        @Override
        public final void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            this.accumulatingSink.item(loop, item);
        }

        @Override
        public final BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            GambitCreator.ScopeBuilder subScope = scope.scope();
            BytecodeExpression accumulated = this.accumulatingSink.end(subScope, loop);
            BytecodeExpression transformed = transform(scope, accumulated);
            GambitCreator.IterateBuilder nextLoop = scope.iterate(transformed);
            this.next.prepare(scope, program, ctxExpr, nextLoop.getItem().getType());
            this.next.item(nextLoop, nextLoop.getItem());
            return this.next.end(scope, nextLoop);
        }

        protected abstract BytecodeExpression transform(GambitCreator.ScopeBuilder scope, BytecodeExpression stream);
    }

    class AccumulatingSink extends BaseStreamSink {
        BytecodeExpression list;

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            super.prepare(scope, program, context, itemType);
            list = scope.evaluateInto(scope.list(itemType));
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            loop.exec(loop.invokeExact(Location.NONE, "add", Collection.class, BaseTypeAdapter.BOOLEAN, list, loop.cast(Location.NONE, AnyTypeWidget.getInstance(), item)));
        }

        @Override
        public BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            return scope.complete(loop.build(list));
        }
    }

    class StreamingSink extends BaseStreamSink {
        BytecodeExpression target;

        StreamingSink(BytecodeExpression target) {
            this.target = target;
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            loop.exec(loop.invoke(Location.NONE,
                    ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "receive", target.getType(), BaseTypeAdapter.BOOLEAN, target, loop.cast(AnyTypeWidget.getInstance(), item))));
        }

        @Override
        public BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            return scope.complete(loop.build());
        }
    }


    /**
     * An operator implementation to group input rows by an extracted key.
     * <p/>
     * There is a decision here -- do we require the input to be sorted by the group key? If so, we can operate in a streaming manner. If not, then we must accumulate all output before emitting any.
     * Decision: Err on the side of ease of use and do the accumulation. Later we can define a SORTED_GROUPBY operator for the optimization case when the constructor knows the input is sorted.
     */
    class GroupbySink extends BaseStreamSink {
        private final StreamSink next;
        private final OperatorNode<FunctionOperator> key;
        private final OperatorNode<FunctionOperator> output;

        private GambitCreator.Invocable compiledKey;
        private GambitCreator.Invocable compiledOutput;
        private TypeWidget listOfItemType;
        private TypeWidget accumulateMapType;
        private BytecodeExpression map;

        public GroupbySink(StreamSink next, OperatorNode<FunctionOperator> key, OperatorNode<FunctionOperator> output) {
            this.next = next;
            this.key = key;
            this.output = output;
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            this.compiledKey = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType), key);
            this.listOfItemType = new ListTypeWidget(itemType);
            this.compiledOutput = compileFunction(program.getType(), context.getType(), ImmutableList.of(compiledKey.getReturnType(), listOfItemType), output);
            this.accumulateMapType = new MapTypeWidget(Type.getType(LinkedHashMap.class), compiledKey.getReturnType(), listOfItemType);
            map = scope.evaluateInto(accumulateMapType.construct());
            super.prepare(scope, program, context, itemType);
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            // key = keyfunction(item)
            // if map.containsKey(key)
            //    list = map.get(key)
            // else
            //    list = new list
            //    map.put(key, list)
            // list.add(item)
            BytecodeExpression key = loop.evaluateInto(loop.cast(AnyTypeWidget.getInstance(), loop.invoke(this.key.getLocation(), compiledKey, program, ctxExpr, item)));
            GambitCreator.CaseBuilder test = loop.createCase();
            test.when(loop.invokeExact(this.key.getLocation(), "containsKey", Map.class, BaseTypeAdapter.BOOLEAN, map, key),
                    loop.cast(listOfItemType, loop.invokeExact(this.key.getLocation(), "get", Map.class, AnyTypeWidget.getInstance(), map, key)));
            GambitCreator.ScopeBuilder scope = loop.scope();
            BytecodeExpression list = scope.evaluateInto(listOfItemType.construct());
            scope.exec(scope.invokeExact(this.key.getLocation(), "put", Map.class, AnyTypeWidget.getInstance(), map, key, scope.cast(AnyTypeWidget.getInstance(), list)));
            BytecodeExpression targetList = loop.evaluateInto(test.exit(scope.complete(list)));
            loop.exec(loop.invokeExact(Location.NONE, "add", Collection.class, BaseTypeAdapter.BOOLEAN, targetList, loop.cast(Location.NONE, AnyTypeWidget.getInstance(), item)));
        }

        @Override
        public BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            // prepare next
            // loop through map key/values, call output function, pass along to next
            scope.exec(loop.build());
            GambitCreator.IterateBuilder nextLoop = scope.iterate(map);
            BytecodeExpression item = nextLoop.getItem();
            PropertyAdapter entryType = nextLoop.getItem().getType().getPropertyAdapter();
            BytecodeExpression actualItem = nextLoop.evaluateInto(nextLoop.invoke(this.output.getLocation(), compiledOutput, program, ctxExpr, entryType.property(item, "key"), entryType.property(item, "value")));
            this.next.prepare(scope, program, ctxExpr, actualItem.getType());
            this.next.item(nextLoop, actualItem);
            return this.next.end(scope, nextLoop);
        }
    }

    /**
     * An operator implementation to group input rows by an extracted key.
     * <p/>
     * There is a decision here -- do we require the input to be sorted by the group key? If so, we can operate in a streaming manner. If not, then we must accumulate all output before emitting any.
     * Decision: Err on the side of ease of use and do the accumulation. Later we can define a SORTED_GROUPBY operator for the optimization case when the constructor knows the input is sorted.
     */
    class HashJoinSink extends BaseStreamSink {
        private boolean outer;
        private final StreamSink next;
        private final OperatorNode<PhysicalExprOperator> right;
        private final OperatorNode<FunctionOperator> leftKey;
        private final OperatorNode<FunctionOperator> rightKey;
        private final OperatorNode<FunctionOperator> join;
        private GambitCreator.Invocable compiledLeftKey;
        private GambitCreator.Invocable compiledJoin;
        private TypeWidget listOfRightType;
        private TypeWidget rightRowType;
        private BytecodeExpression emptyMatchList;
        private BytecodeExpression rightMap;

        public HashJoinSink(StreamSink next, boolean outer, OperatorNode<PhysicalExprOperator> right, OperatorNode<FunctionOperator> leftKey, OperatorNode<FunctionOperator> rightKey, OperatorNode<FunctionOperator> join) {
            this.outer = outer;
            this.next = new SkipNullsSink(next);
            this.right = right;
            this.leftKey = leftKey;
            this.rightKey = rightKey;
            this.join = join;
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            super.prepare(scope, program, context, itemType);
            BytecodeExpression rightExpr = scope.local(evaluateExpression(program, context, right));
            IterateAdapter rightIterator = rightExpr.getType().getIterableAdapter();
            this.rightRowType = outer ? NullableTypeWidget.create(rightIterator.getValue()) : NotNullableTypeWidget.create(rightIterator.getValue());
            this.listOfRightType = new ListTypeWidget(this.rightRowType);
            GambitCreator.Invocable compiledRightKey = compileFunction(program.getType(), context.getType(), ImmutableList.of(rightRowType), rightKey);
            TypeWidget rightMapType = new MapTypeWidget(Type.getType(HashMap.class), compiledRightKey.getReturnType(), listOfRightType);
            this.rightMap = scope.evaluateInto(rightMapType.construct());
            if(outer) {
                this.emptyMatchList = scope.constant(listOfRightType, Arrays.asList(new Object[1]));
            } else {
                this.emptyMatchList = scope.constant(listOfRightType, ImmutableList.of());
            }
            // for item in rightExpr:
            GambitCreator.IterateBuilder loop = scope.iterate(rightExpr);
            //    key = right_key(item)
            BytecodeExpression key = loop.evaluateInto(loop.cast(AnyTypeWidget.getInstance(), loop.invoke(this.rightKey.getLocation(), compiledRightKey, program, context, loop.getItem())));
            GambitCreator.CaseBuilder test = loop.createCase();
            //    if key in map:
            //       list = map[key]
            test.when(scope.invokeExact(this.rightKey.getLocation(), "containsKey", Map.class, BaseTypeAdapter.BOOLEAN, rightMap, key),
                    scope.cast(this.listOfRightType, loop.invokeExact(this.rightKey.getLocation(), "get", Map.class, AnyTypeWidget.getInstance(), rightMap, key)));
            GambitCreator.ScopeBuilder listCreateScope = loop.scope();
            BytecodeExpression list = listCreateScope.evaluateInto(this.listOfRightType.construct());
            listCreateScope.exec(listCreateScope.invokeExact(this.rightKey.getLocation(), "put", Map.class, AnyTypeWidget.getInstance(), rightMap, key, listCreateScope.cast(AnyTypeWidget.getInstance(), list)));
            //    else:
            //       list = new list
            //       map[key] = list
            BytecodeExpression targetList = loop.evaluateInto(test.exit(listCreateScope.complete(list)));
            //    list.add(item)
            loop.exec(loop.invokeExact(Location.NONE, "add", Collection.class, BaseTypeAdapter.BOOLEAN, targetList, loop.cast(Location.NONE, AnyTypeWidget.getInstance(), loop.getItem())));
            scope.exec(loop.build());
            this.compiledLeftKey = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType), leftKey);
            this.compiledJoin = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType, this.rightRowType), join);
            this.next.prepare(scope, program, context, compiledJoin.getReturnType());
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            BytecodeExpression key = loop.evaluateInto(loop.cast(AnyTypeWidget.getInstance(), loop.invoke(this.leftKey.getLocation(), compiledLeftKey, program, ctxExpr, item)));
            //
            // arrange for the output list to container either:
            //     1) list of right rows or
            //     2) empty list or list of a single null (depending on outer join)
            GambitCreator.CaseBuilder test = loop.createCase();
            test.when(loop.invokeExact(this.leftKey.getLocation(), "containsKey", Map.class, BaseTypeAdapter.BOOLEAN, rightMap, key),
                    loop.cast(listOfRightType, loop.invokeExact(this.leftKey.getLocation(), "get", Map.class, AnyTypeWidget.getInstance(), rightMap, key)));
            BytecodeExpression rightMatchedList = loop.evaluateInto(test.exit(emptyMatchList));
            GambitCreator.IterateBuilder rightLoop = loop.iterate(rightMatchedList);
            this.next.item(rightLoop, rightLoop.evaluateInto(rightLoop.invoke(join.getLocation(), compiledJoin, program, ctxExpr, item, rightLoop.getItem())));
            loop.exec(rightLoop.build());
        }

        @Override
        public BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            return this.next.end(scope, loop);
        }
    }

    private class TransformSink extends BaseTransformSink {
        private GambitCreator.Invocable compiledTransform;
        private final OperatorNode<FunctionOperator> function;

        public TransformSink(StreamSink next, OperatorNode<FunctionOperator> function) {
            super(next);
            this.function = function;
        }


        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            compiledTransform = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType), function);
            super.prepare(scope, program, context, compiledTransform.getReturnType());
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            super.item(loop, loop.evaluateInto(loop.invoke(function.getLocation(), compiledTransform, program, ctxExpr, item)));
        }
    }

    private class ScatterSink extends AccumulatedTransformSink {
        private final OperatorNode<FunctionOperator> function;

        public ScatterSink(StreamSink next, OperatorNode<FunctionOperator> function) {
            super(next);
            this.function = function;
        }


        @Override
        protected BytecodeExpression transform(GambitCreator.ScopeBuilder scope, BytecodeExpression output) {
            TypeWidget cursorType = output.getType();
            if (!cursorType.isIterable()) {
                throw new ProgramCompileException("Accumulated output is not iterable: " + cursorType);
            }
            IterateAdapter it = cursorType.getIterableAdapter();
            TypeWidget valueType = it.getValue();
            LambdaFactoryBuilder functor = scope.createLambdaBuilder(Supplier.class, "get", Object.class, true);
            BytecodeExpression programArgument = functor.addArgument("$program", program.getType());
            BytecodeExpression ctxArgument = functor.addArgument("$context", ctxExpr.getType());
            List<String> names = function.getArgument(0);
            Preconditions.checkArgument(names.size() == 1);
            functor.addArgument(names.get(0), NotNullableTypeWidget.create(valueType));
            OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
            GambitCreator.ScopeBuilder funcBody = functor.scope();
            PhysicalExprOperatorCompiler functionCompiler = new PhysicalExprOperatorCompiler(funcBody);
            BytecodeExpression timeout = scope.propertyValue(function.getLocation(), ctxExpr, "timeout");
            BytecodeExpression outputValue =  scope.resolve(function.getLocation(),  timeout, functionCompiler.evaluateExpression(programArgument, ctxArgument, functionBody));
            BytecodeExpression scatterValue = funcBody.complete(outputValue);
            LambdaInvocable invocable = functor.complete(scatterValue);
            GambitCreator.Invocable scatterFunction = invocable.prefix(program, ctxExpr);
            BytecodeExpression tasks = scope.transform(function.getLocation(), output, scatterFunction);
            GambitCreator.Invocable work = scope.findExactInvoker(TaskContext.class, "scatter", new ListTypeWidget(scatterValue.getType()), List.class);
            return scope.cast(new ListTypeWidget(invocable.getResultType()), work.invoke(function.getLocation(), ctxExpr, tasks));
        }
    }

    private class SortSink extends AccumulatedTransformSink {
        private final OperatorNode<FunctionOperator> comparator;

        public SortSink(StreamSink next, OperatorNode<FunctionOperator> comparator) {
            super(next);
            this.comparator = comparator;
        }

        @Override
        protected BytecodeExpression transform(GambitCreator.ScopeBuilder scope, BytecodeExpression output) {
            TypeWidget cursorType = output.getType();
            IterateAdapter it = cursorType.getIterableAdapter();
            TypeWidget valueType = it.getValue();
            LambdaInvocable comparatorType = compileComparator(program.getType(), ctxExpr.getType(), valueType, comparator);
            BytecodeExpression comparatorInstance = scope.invoke(comparator.getLocation(),
                    comparatorType, program, ctxExpr);
            BytecodeExpression sorted = scope.invokeExact(comparator.getLocation(), "sort", ProgramInvocation.class, output.getType(),
                    program,
                    output,
                    new BytecodeCastExpression(scope.adapt(Comparator.class, false), comparatorInstance));
            return sorted;
        }
    }

    private class DistinctSink extends BaseTransformSink {
        Location location;
        AssignableValue set;

        private DistinctSink(StreamSink next, Location loc) {
            super(next);
            this.location = loc;
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            set = scope.local(scope.adapt(HashSet.class, false).construct());
            super.prepare(scope, program, context, itemType);
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            loop.next(loop.not(location, loop.invokeExact(location, "add", HashSet.class, BaseTypeAdapter.BOOLEAN, set, loop.cast(AnyTypeWidget.getInstance(), item))));
            super.item(loop, item);
        }
    }

    private class FlattenSink extends BaseTransformSink {
        private FlattenSink(StreamSink next) {
            super(next);
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            super.prepare(scope, program, context, itemType.getIterableAdapter().getValue());
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            GambitCreator.IterateBuilder iterateBuilder = loop.iterate(item);
            super.item(iterateBuilder, iterateBuilder.getItem());
            loop.exec(iterateBuilder.build());
        }
    }

    private class CrossSink extends BaseTransformSink {
        private final OperatorNode<PhysicalExprOperator> right;
        private final OperatorNode<FunctionOperator> output;
        private BytecodeExpression rightExpr;
        private IterateAdapter rightIterator;
        private IterateAdapter outputIterator;
        private GambitCreator.Invocable compiledOutput;

        public CrossSink(StreamSink next, OperatorNode<PhysicalExprOperator> right, OperatorNode<FunctionOperator> output) {
            super(next);
            this.right = right;
            this.output = output;
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            rightExpr = scope.local(evaluateExpression(program, context, right));
            rightIterator = rightExpr.getType().getIterableAdapter();
            compiledOutput = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType, rightIterator.getValue()), output);
            outputIterator = compiledOutput.getReturnType().getIterableAdapter();
            super.prepare(scope, program, context, outputIterator.getValue());
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            // for each item in right
            //    emit each item in output(item, right)
            GambitCreator.IterateBuilder rightIterator = loop.iterate(rightExpr);
            BytecodeExpression rows = rightIterator.evaluateInto(rightIterator.invoke(right.getLocation(), compiledOutput, program, ctxExpr, item, rightIterator.getItem()));
            GambitCreator.IterateBuilder outputIterator = rightIterator.iterate(rows);
            super.item(outputIterator, outputIterator.getItem());
            rightIterator.exec(outputIterator.build());
            loop.exec(rightIterator.build());
        }

        @Override
        public BytecodeExpression end(GambitCreator.ScopeBuilder scope, GambitCreator.IterateBuilder loop) {
            return super.end(scope, loop);
        }
    }

    private StreamSink compileStream(BytecodeExpression program, BytecodeExpression ctxExpr, OperatorNode<StreamOperator> stream) {
        if (stream.getOperator() == StreamOperator.SINK) {
            OperatorNode<SinkOperator> sink = stream.getArgument(0);
            switch (sink.getOperator()) {
                case ACCUMULATE:
                    return new AccumulatingSink();
                case STREAM: {
                    OperatorNode<PhysicalExprOperator> target = sink.getArgument(0);
                    BytecodeExpression targetExpression = evaluateExpression(program, ctxExpr, target);
                    return new StreamingSink(targetExpression);
                }
                default:
                    throw new UnsupportedOperationException("Unknown SINK operator: " + sink);
            }
        }
        StreamSink next = compileStream(program, ctxExpr, stream.getArgument(0));
        switch (stream.getOperator()) {
            case TRANSFORM: {
                OperatorNode<FunctionOperator> function = stream.getArgument(1);
                return new TransformSink(next, function);
            }
            case SCATTER: {
                OperatorNode<FunctionOperator> function = stream.getArgument(1);
                return new ScatterSink(next, function);
            }
            case DISTINCT: {
                return new DistinctSink(next, stream.getLocation());
            }
            case FLATTEN: {
                return new FlattenSink(next);
            }
            case FILTER: {
                OperatorNode<FunctionOperator> function = stream.getArgument(1);
                return new FilterSink(next, function);
            }
            case OFFSET: {
                OperatorNode<PhysicalExprOperator> offset = stream.getArgument(1);
                return new SliceSink(next, null, offset);
            }
            case LIMIT: {
                OperatorNode<PhysicalExprOperator> limit = stream.getArgument(1);
                return new SliceSink(next, limit, null);
            }
            case SLICE: {
                OperatorNode<PhysicalExprOperator> offset = stream.getArgument(1);
                OperatorNode<PhysicalExprOperator> limit = stream.getArgument(2);
                return new SliceSink(next, limit, offset);
            }
            case ORDERBY: {
                OperatorNode<FunctionOperator> comparator = stream.getArgument(1);
                return new SortSink(next, comparator);
            }
            case GROUPBY: {
                OperatorNode<FunctionOperator> key = stream.getArgument(1);
                OperatorNode<FunctionOperator> output = stream.getArgument(2);
                return new GroupbySink(next, key, output);
            }
            case CROSS: {
                OperatorNode<PhysicalExprOperator> right = stream.getArgument(1);
                OperatorNode<FunctionOperator> output = stream.getArgument(2);
                return new CrossSink(next, right, output);
            }
            case OUTER_HASH_JOIN:
            case HASH_JOIN: {
                OperatorNode<PhysicalExprOperator> right = stream.getArgument(1);
                OperatorNode<FunctionOperator> leftKey = stream.getArgument(2);
                OperatorNode<FunctionOperator> rightKey = stream.getArgument(3);
                OperatorNode<FunctionOperator> join = stream.getArgument(4);
                return new HashJoinSink(next, stream.getOperator() == StreamOperator.OUTER_HASH_JOIN, right, leftKey, rightKey, join);
            }
            default:
                throw new UnsupportedOperationException("Unexpected transform StreamOperator: " + stream.toString());
        }
    }

    private class FilterSink extends BaseTransformSink {
        private final OperatorNode<FunctionOperator> function;

        public FilterSink(StreamSink next, OperatorNode<FunctionOperator> function) {
            super(next);
            this.function = function;
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            final GambitCreator.Invocable compiledPredicate = compileFunction(program.getType(), ctxExpr.getType(), ImmutableList.of(item.getType()), function);
            loop.next(loop.not(function.getLocation(), loop.invoke(function.getLocation(), compiledPredicate, program, ctxExpr, item)));
            super.item(loop, item);
        }
    }

    private class SkipNullsSink extends BaseTransformSink {
        public SkipNullsSink(StreamSink next) {
            super(next);
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            super.prepare(scope, program, context, NotNullableTypeWidget.create(itemType));
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            item = loop.evaluateInto(item);
            loop.next(loop.isNull(Location.NONE, item));
            super.item(loop, new NullTestedExpression(item));
        }
    }

    private class SliceSink extends BaseTransformSink {
        private final OperatorNode<PhysicalExprOperator> limit;
        private final OperatorNode<PhysicalExprOperator> offset;
        AssignableValue off;
        AssignableValue lmt;

        public SliceSink(StreamSink next, OperatorNode<PhysicalExprOperator> limit, OperatorNode<PhysicalExprOperator> offset) {
            super(next);
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public void prepare(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression context, TypeWidget itemType) {
            if (offset != null) {
                off = scope.local(evaluateExpression(program, context, offset));
            }
            if (limit != null) {
                lmt = scope.local(evaluateExpression(program, context, limit));
            }
            super.prepare(scope, program, context, itemType);
        }

        @Override
        public void item(GambitCreator.IterateBuilder loop, BytecodeExpression item) {
            if (offset != null) {
                loop.inc(off, -1);
                loop.next(loop.compare(offset.getLocation(), BinaryComparison.GTEQ, off, loop.constant(0)));
            }
            if (limit != null) {
                loop.abort(loop.compare(limit.getLocation(), BinaryComparison.LT, lmt, loop.constant(1)));
                loop.inc(lmt, -1);
            }
            super.item(loop, item);
        }
    }
}

