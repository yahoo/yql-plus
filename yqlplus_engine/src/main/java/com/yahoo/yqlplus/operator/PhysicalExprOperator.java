/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.compiler.runtime.ArithmeticOperation;
import com.yahoo.yqlplus.engine.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.operator.OperatorNodeVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public enum PhysicalExprOperator implements Operator {
    // end(context, input-value)
    END_CONTEXT(PhysicalExprOperator.class),
    // TIMEOUT(timeout, units)
    TIMEOUT_MAX(PhysicalExprOperator.class, PhysicalExprOperator.class),
    // TRACE(record<string>string dimensions)
    TRACE_CONTEXT(PhysicalExprOperator.class),

    TIMEOUT_REMAINING(TimeUnit.class),

    WITH_CONTEXT(PhysicalExprOperator.class, PhysicalExprOperator.class),
    ENFORCE_TIMEOUT(PhysicalExprOperator.class),

    // STREAM_CREATE(operators) -> stream<rowtype>
    STREAM_CREATE(StreamOperator.class),

    STREAM_EXECUTE(PhysicalExprOperator.class, StreamOperator.class),

    // STREAM_COMPLETE(context, target-stream, dependent-add-values) -> async array<rowtype>
    STREAM_COMPLETE(PhysicalExprOperator.class, PlanOperatorTypes.VALUES),

    AND(PlanOperatorTypes.EXPRS),
    OR(PlanOperatorTypes.EXPRS),

    EQ(PhysicalExprOperator.class, PhysicalExprOperator.class),
    NEQ(PhysicalExprOperator.class, PhysicalExprOperator.class),
    BOOLEAN_COMPARE(BinaryComparison.class, PhysicalExprOperator.class, PhysicalExprOperator.class),
    IN(PhysicalExprOperator.class, PhysicalExprOperator.class),
    IS_NULL(PhysicalExprOperator.class),
    MATCHES(PhysicalExprOperator.class, PhysicalExprOperator.class),
    CONTAINS(PhysicalExprOperator.class, PhysicalExprOperator.class),

    BINARY_MATH(ArithmeticOperation.class, PhysicalExprOperator.class, PhysicalExprOperator.class),
    COMPARE(PhysicalExprOperator.class, PhysicalExprOperator.class),
    MULTICOMPARE(PlanOperatorTypes.EXPRS),
    COALESCE(PlanOperatorTypes.EXPRS),
    IF(PhysicalExprOperator.class, PhysicalExprOperator.class, PhysicalExprOperator.class),

    NEGATE(PhysicalExprOperator.class),
    NOT(PhysicalExprOperator.class),

    RECORD(TypeCheckers.LIST_OF_STRING, PlanOperatorTypes.EXPRS),
    PROJECT(new TypeLiteral<List<OperatorNode<PhysicalProjectOperator>>>() {
    }),

    ARRAY(PlanOperatorTypes.EXPRS),
    RESOLVE(PhysicalExprOperator.class),

    INDEX(PhysicalExprOperator.class, PhysicalExprOperator.class),

    PROPREF(PhysicalExprOperator.class, String.class),
    PROPREF_DEFAULT(PhysicalExprOperator.class, String.class, PhysicalExprOperator.class),

    RECORD_AS(Type.class, TypeCheckers.LIST_OF_STRING, PlanOperatorTypes.EXPRS),
    RECORD_FROM(Type.class, PhysicalExprOperator.class),
    // (returnType, owner, methodName, methodDescriptor)
    INVOKEVIRTUAL(java.lang.reflect.Type.class, Type.class, String.class, String.class, PlanOperatorTypes.EXPRS),
    INVOKESTATIC(java.lang.reflect.Type.class, Type.class, String.class, String.class, PlanOperatorTypes.EXPRS),
    INVOKEINTERFACE(java.lang.reflect.Type.class, Type.class, String.class, String.class, PlanOperatorTypes.EXPRS),
    INVOKENEW(java.lang.reflect.Type.class, PlanOperatorTypes.EXPRS),

    CONSTANT(TypeWidget.class, Object.class),
    // CONSTANT_VALUE(genericType, value)
    CONSTANT_VALUE(java.lang.reflect.Type.class, Object.class),
    NULL(TypeWidget.class),
    CAST(TypeWidget.class, PhysicalExprOperator.class),
    NEW(TypeWidget.class, PlanOperatorTypes.EXPRS),

    VALUE(OperatorValue.class),
    LOCAL(String.class),

    GENERATE_KEYS(TypeCheckers.LIST_OF_STRING, PlanOperatorTypes.EXPRS),

    BOOL(PhysicalExprOperator.class),

    CATCH(PhysicalExprOperator.class, PhysicalExprOperator.class),

    CURRENT_CONTEXT(),
    FIRST(PhysicalExprOperator.class),
    LENGTH(PhysicalExprOperator.class),
    SINGLETON(PhysicalExprOperator.class),
    THROW(PhysicalExprOperator.class);

    private final ArgumentsTypeChecker checker;


    PhysicalExprOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }

    public static MethodInvoker createInvoker(Method method) {
        PhysicalExprOperator callOperator = determineCallOperator(method);
        java.lang.reflect.Type returnType = method.getGenericReturnType();
        // (returnType, owner, methodName, methodDescriptor)
        Type owner = Type.getType(method.getDeclaringClass());
        String methodName = method.getName();
        String methodDescriptor = Type.getMethodDescriptor(method);
        return new MethodInvoker() {
            @Override
            public boolean isStatic() {
                return callOperator == PhysicalExprOperator.INVOKESTATIC;
            }

            @Override
            public OperatorNode<PhysicalExprOperator> invoke(List<OperatorNode<PhysicalExprOperator>> args) {
                return OperatorNode.create(callOperator, returnType, owner, methodName, methodDescriptor, args);
            }
        };
    }

    private static PhysicalExprOperator determineCallOperator(Method method) {
        PhysicalExprOperator callOperator = PhysicalExprOperator.INVOKEVIRTUAL;
        if (Modifier.isStatic(method.getModifiers())) {
            callOperator = PhysicalExprOperator.INVOKESTATIC;
        } else if (method.getDeclaringClass().isInterface()) {
            callOperator = PhysicalExprOperator.INVOKEINTERFACE;
        }
        return callOperator;
    }

    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }


    public List<String> localsFor(OperatorNode<PhysicalExprOperator> node) {
        final Set<String> result = Sets.newHashSet();
        node.visitNode(new OperatorNodeVisitor() {
            @Override
            public void visit(Object arg) {

            }

            @Override
            public <T extends Operator> boolean enter(OperatorNode<T> node) {
                return !(node.getOperator() instanceof FunctionOperator);
            }

            @Override
            public <T extends Operator> void exit(OperatorNode<T> node) {
                if (node.getOperator() instanceof PhysicalExprOperator) {
                    OperatorNode<PhysicalExprOperator> expr = (OperatorNode<PhysicalExprOperator>) node;
                    switch (expr.getOperator()) {
                        case LOCAL: {
                            String name = expr.getArgument(0);
                            result.add(name);
                        }
                    }
                }
            }
        });
        return ImmutableList.copyOf(result);
    }
}
