/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.TypeLiteral;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.MetricEmitter;
import com.yahoo.yqlplus.api.annotations.*;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitCreator;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitScope;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitTypes;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ObjectBuilder;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ScopedBuilder;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.language.parser.Location;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SourceApiGenerator {
    protected final GambitScope gambitScope;
    int sym = 0;

    public SourceApiGenerator(GambitScope gambitScope) {
        this.gambitScope = gambitScope;
    }

    protected String gensym(String prefix) {
        return prefix + (++sym);
    }

    protected BytecodeExpression metricWith(ScopedBuilder cb, BytecodeExpression metric, String key, String value) {
        return cb.invokeExact(Location.NONE, "with", MetricDimension.class, cb.adapt(MetricDimension.class, false), metric, gambitScope.constant(key), gambitScope.constant(value));
    }

    interface SourceArgumentVisitor {
        BytecodeExpression visitKeyArgument(Key key, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget);

        BytecodeExpression visitCompoundKey(CompoundKey compoundKey, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget);

        BytecodeExpression visitSet(Set annotate, DefaultValue defaultValue, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget);
    }

    protected void reportMethodParameterException(String type, Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("@%s method error: %s.%s: %s", type, method.getDeclaringClass().getName(), method.getName(), message));
    }

    private void reportMethodException(Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("method error: %s.%s: %s", method.getDeclaringClass().getName(), method.getName(), message));
    }

    private ObjectBuilder.FieldBuilder createInjectedField(ObjectBuilder target, GambitTypes gambitScope, java.lang.reflect.Type genericType, Annotation[] annotation) {
        TypeLiteral<?> genericParam = TypeLiteral.get(genericType);
        ObjectBuilder.FieldBuilder injectedField = target.field(gensym("inject$"), gambitScope.adapt(genericParam.getRawType(), false));
        injectedField.annotate(Inject.class);
        Annotation bindingAnnotation = null;
        for (Annotation ann : annotation) {
            if (ann.getClass().isAnnotationPresent(BindingAnnotation.class)) {
                Preconditions.checkArgument(bindingAnnotation == null, "Already found a binding annotation %s and now found another %s: that's too many", bindingAnnotation, ann);
                bindingAnnotation = ann;
            }
        }
        if (bindingAnnotation != null) {
            injectedField.annotate(bindingAnnotation);
        }
        return injectedField;
    }

    protected void visitMethodArguments(ObjectBuilder target, Method method, SourceArgumentVisitor bodyBuilder, AssignableValue contextVar, ScopedBuilder catchBody, Iterator<String> freeArguments, List<BytecodeExpression> invocationArguments, GambitCreator.ScopeBuilder block) {
        Class<?>[] arguments = method.getParameterTypes();
        java.lang.reflect.Type[] genericArguments = method.getGenericParameterTypes();
        Annotation[][] annotations = method.getParameterAnnotations();
        for (int i = 0; i < arguments.length; ++i) {
            Class<?> parameterType = arguments[i];
            if (isFreeArgument(parameterType, annotations[i])) {
                invocationArguments.add(catchBody.local(freeArguments.next()));
                continue;
            }
            TypeWidget parameterWidget = gambitScope.adapt(genericArguments[i], true);
            for (Annotation annotate : annotations[i]) {
                if (annotate instanceof Key) {
                    invocationArguments.add(bodyBuilder.visitKeyArgument(((Key) annotate), catchBody, parameterType, parameterWidget));
                } else if (annotate instanceof CompoundKey) {
                    invocationArguments.add(bodyBuilder.visitCompoundKey(((CompoundKey) annotate), catchBody, parameterType, parameterWidget));
                } else if (annotate instanceof Set) {
                    DefaultValue defaultValue = null;
                    for (Annotation ann : annotations[i]) {
                        if (ann instanceof DefaultValue) {
                            defaultValue = (DefaultValue) ann;
                        }
                    }
                    invocationArguments.add(bodyBuilder.visitSet(((Set) annotate), defaultValue, catchBody, parameterType, parameterWidget));
                } else if (annotate instanceof TimeoutMilliseconds) {
                    if (!Long.TYPE.isAssignableFrom(parameterType)) {
                        reportMethodParameterException("TimeoutMilliseconds", method, "@TimeoutMilliseconds argument type must be a primitive long");
                    }
                    BytecodeExpression timeoutExpr = catchBody.propertyValue(Location.NONE, contextVar, "timeout");
                    invocationArguments.add(catchBody.invokeExact(Location.NONE, "getRemaining", Timeout.class, BaseTypeAdapter.INT64, timeoutExpr, catchBody.constant(TimeUnit.MILLISECONDS)));
                } else if (annotate instanceof Injected) {
                    ObjectBuilder.FieldBuilder fld = createInjectedField(target, catchBody, genericArguments[i], annotations[i]);
                    invocationArguments.add(fld.get(catchBody.local("this")));
                } else if (annotate instanceof Trace) {
                    if (!Tracer.class.isAssignableFrom(parameterType)) {
                        reportMethodParameterException("Trace", method, "@Trace argument type must be a %s", Tracer.class.getName());
                    }
                    Trace traceAnnotation = (Trace) annotate;
                    String group = traceAnnotation.group();
                    if ("".equals(group)) {
                        group = method.getDeclaringClass().getName() + "::" + method.getName();
                    }
                    BytecodeExpression tracerExpr = catchBody.propertyValue(Location.NONE, contextVar, "tracer");
                    BytecodeExpression methodTracerExpr = catchBody.invokeExact(Location.NONE, "start", Tracer.class, tracerExpr.getType(), tracerExpr, catchBody.constant(group), catchBody.constant(traceAnnotation.value()));
                    AssignableValue methodTracerVar = block.allocate("methodTracer", gambitScope.adapt(Tracer.class, false));
                    block.set(Location.NONE, methodTracerVar, methodTracerExpr);
                    invocationArguments.add(methodTracerVar);
                } else if (annotate instanceof Emitter) {
                    if (!MetricEmitter.class.isAssignableFrom(parameterType)) {
                        reportMethodParameterException("Trace", method, "@Emitter argument type must be a %s", MetricEmitter.class.getName());
                    }
                    invocationArguments.add(catchBody.propertyValue(Location.NONE, contextVar, "metricEmitter"));
                } else if (annotate instanceof DefaultValue) {
                    // just make sure it's paired with a Set
                    boolean found = false;
                    for (Annotation ann : annotations[i]) {
                        if (ann instanceof Set) {
                            found = true;
                        }
                    }
                    if (!found) {
                        reportMethodException(method, "Declares @DefaultValue('%s') parameter without @Set", ((DefaultValue) annotate).value());
                    }
                }
            }
        }
    }

    private static final List<Class<?>> SOURCE_ANNOTATIONS =
            ImmutableList.of(com.yahoo.yqlplus.api.annotations.Key.class,
                    Set.class,
                    DefaultValue.class,
                    CompoundKey.class,
                    TimeoutMilliseconds.class,
                    Injected.class,
                    Trace.class,
                    Emitter.class);

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


}
