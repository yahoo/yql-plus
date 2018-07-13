/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.cloud.metrics.api.MetricEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Emitter;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.annotations.TimeoutMilliseconds;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.CompileContext;
import com.yahoo.yqlplus.engine.ModuleType;
import com.yahoo.yqlplus.engine.StreamValue;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.OperatorStep;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.PhysicalOperator;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ExportModuleAdapter implements ModuleType {
    private static final List<Class<?>> SOURCE_ANNOTATIONS =
            ImmutableList.of(Key.class,
                    Set.class,
                    TimeoutMilliseconds.class,
                    Emitter.class);
    private final String moduleName;
    private final Class<?> clazz;
    private final Supplier<?> supplier;
    private OperatorNode<PhysicalExprOperator> module;

    public ExportModuleAdapter(String moduleName, Class<?> clazz, Supplier<?> module) {
        this.moduleName = moduleName;
        this.clazz = clazz;
        this.supplier = module;
    }

    public ExportModuleAdapter(String moduleName, Class<?> clazz) {
        this.moduleName = moduleName;
        this.clazz = clazz;
        this.supplier = null;
    }

    protected static boolean isFreeArgument(Class<?> argumentType, Annotation[] annotations) {
        for (Annotation annotate : annotations) {
            for (Class<?> clazz : SOURCE_ANNOTATIONS) {
                if (clazz.isInstance(annotate)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> call(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments) {
        return adaptInvoke(location, context, name, null, arguments, null);
    }

    private OperatorNode<PhysicalExprOperator> adaptInvoke(Location location, CompileContext context, String name, OperatorNode<PhysicalExprOperator> streamInput, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row) {
        List<OperatorNode<PhysicalExprOperator>> inputArgs = context.evaluateAllInRowContext(arguments, row);
        if(streamInput != null) {
            inputArgs.add(0, streamInput);
        }
        methods:
        for (Method method : clazz.getMethods()) {
            if (!method.isAnnotationPresent(Export.class)) {
                continue;
            }
            if (!name.equalsIgnoreCase(method.getName())) {
                continue;
            }
            List<OperatorNode<PhysicalExprOperator>> invokeArguments = Lists.newArrayList();
            PhysicalExprOperator callOperator = PhysicalExprOperator.INVOKEVIRTUAL;
            if (Modifier.isStatic(method.getModifiers())) {
                callOperator = PhysicalExprOperator.INVOKESTATIC;
            } else if(clazz.isInterface()) {
                callOperator = PhysicalExprOperator.INVOKEINTERFACE;
                invokeArguments.add(getModule(location, context));
            } else {
                invokeArguments.add(getModule(location, context));
            }
            Class<?>[] argumentTypes = method.getParameterTypes();
            Annotation[][] annotations = method.getParameterAnnotations();
            Iterator<OperatorNode<PhysicalExprOperator>> inputNext = inputArgs.iterator();
            for (int i = 0; i < argumentTypes.length; ++i) {
                Class<?> parameterType = argumentTypes[i];
                if (isFreeArgument(argumentTypes[i], annotations[i])) {
                    if (!inputNext.hasNext()) {
                        continue methods;
                    }
                    invokeArguments.add(inputNext.next());
                } else {
                    for (Annotation annotate : annotations[i]) {
                        if (annotate instanceof Key) {
                            continue methods;
                        } else if (annotate instanceof Set || annotate instanceof DefaultValue) {
                            continue methods;
                        } else if (annotate instanceof TimeoutMilliseconds) {
                            if (!Long.TYPE.isAssignableFrom(parameterType)) {
                                reportMethodParameterException("TimeoutMilliseconds", method, "@TimeoutMilliseconds argument type must be a primitive long");
                            }
                            invokeArguments.add(OperatorNode.create(PhysicalExprOperator.TIMEOUT_REMAINING, TimeUnit.MILLISECONDS));
                        } else if (annotate instanceof Emitter) {
                            if (MetricEmitter.class.isAssignableFrom(parameterType) || TaskMetricEmitter.class.isAssignableFrom(parameterType)) {
                                invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "metricEmitter"));
                            } else if (Tracer.class.isAssignableFrom(parameterType)) {
                                invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "tracer"));
                            } else {
                                reportMethodParameterException("Trace", method, "@Emitter argument type must be a %s or %s", MetricEmitter.class.getName(), Tracer.class.getName());
                            }
                        }
                    }
                }
            }
            return OperatorNode.create(callOperator, method.getGenericReturnType(), Type.getType(method.getDeclaringClass()), method.getName(), Type.getMethodDescriptor(method), invokeArguments);
        }
        for (Field field : clazz.getFields()) {
            if (!field.isAnnotationPresent(Export.class) || !Modifier.isPublic(field.getModifiers()) || !name.equalsIgnoreCase(field.getName())) {
                continue;
            }
            return OperatorNode.create(PhysicalExprOperator.PROPREF, getModule(location, context), field.getName());
        }

        throw new ProgramCompileException(location, "Method '%s' not found on module %s with matching argument count %d", name, moduleName, arguments.size());
    }

    @Override
    public OperatorNode<PhysicalExprOperator> callInRowContext(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row) {
        return adaptInvoke(location, context, name, null, arguments, row);
    }

    private OperatorNode<PhysicalExprOperator> getModule(Location location, CompileContext planner) {
        if (module == null) {
            if (supplier != null) {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                        OperatorNode.create(location, PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(location, PhysicalExprOperator.INVOKEINTERFACE,
                                clazz, Type.getType(Supplier.class), "get", Type.getMethodDescriptor(Type.getType(Object.class)),
                                ImmutableList.of(OperatorNode.create(PhysicalExprOperator.CONSTANT_VALUE, Supplier.class, supplier))));
                module = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
            }  else {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                        OperatorNode.create(location, PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(location, PhysicalExprOperator.INVOKENEW,
                                clazz,
                                ImmutableList.of()));
                module = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
            }
        }
        return module;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> property(Location location, CompileContext context, String name) {
        return callInRowContext(location, context, name, ImmutableList.of(), null);
    }

    @Override
    public StreamValue pipe(Location location, CompileContext context, String name, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments) {
        return StreamValue.iterate(context, adaptInvoke(location, context, name, input.materializeValue(), arguments, null));
    }

    protected void reportMethodParameterException(String type, Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("@%s method error: %s.%s: %s", type, method.getDeclaringClass().getName(), method.getName(), message));
    }

}
