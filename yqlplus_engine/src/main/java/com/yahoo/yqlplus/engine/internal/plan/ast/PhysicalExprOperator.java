/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.ast;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.compiler.generate.GambitCreator;
import com.yahoo.yqlplus.compiler.runtime.ArithmeticOperation;
import com.yahoo.yqlplus.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.language.logical.ArgumentsTypeChecker;
import com.yahoo.yqlplus.language.logical.TypeCheckers;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.operator.OperatorNodeVisitor;

import java.util.List;
import java.util.Set;

public enum PhysicalExprOperator implements Operator {
    ROOT_CONTEXT(),
    // end(context, input-value)
    END_CONTEXT(PhysicalExprOperator.class),
    // TIMEOUT(timeout, units)
    TIMEOUT_MAX(PhysicalExprOperator.class, PhysicalExprOperator.class),
    // TIMEOUT_GUARD(min-timeout, min-units, max-timeout, max-units)
    TIMEOUT_GUARD(PhysicalExprOperator.class, PhysicalExprOperator.class, PhysicalExprOperator.class, PhysicalExprOperator.class),
    // TRACE(record<string>string dimensions)
    TRACE_CONTEXT(PhysicalExprOperator.class),

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
    RECORD_AS(TypeWidget.class, TypeCheckers.LIST_OF_STRING, PlanOperatorTypes.EXPRS),
    PROJECT(new TypeLiteral<List<OperatorNode<PhysicalProjectOperator>>>() {}),

    ARRAY(PlanOperatorTypes.EXPRS),

    INDEX(PhysicalExprOperator.class, PhysicalExprOperator.class),

    PROPREF(PhysicalExprOperator.class, String.class),

    CALL(TypeWidget.class, String.class, PlanOperatorTypes.EXPRS),
    INVOKE(GambitCreator.Invocable.class, PlanOperatorTypes.EXPRS),
    ASYNC_INVOKE(GambitCreator.Invocable.class, PlanOperatorTypes.EXPRS),

    VALUE(OperatorValue.class),
    LOCAL(String.class),
    CONSTANT(TypeWidget.class, Object.class),
    NULL(TypeWidget.class),

    FOREACH(PhysicalExprOperator.class, FunctionOperator.FUNCTION),
    //LET(FunctionOperator.FUNCTION, PlanOperatorTypes.EXPRS),
    CONCAT(PlanOperatorTypes.EXPRS),

    GENERATE_KEYS(TypeCheckers.LIST_OF_STRING, PlanOperatorTypes.EXPRS),

    BOOL(PhysicalExprOperator.class),

    CATCH(PhysicalExprOperator.class, PhysicalExprOperator.class),

    CAST(TypeWidget.class, PhysicalExprOperator.class),
    CURRENT_CONTEXT(),
    NEW(TypeWidget.class, PlanOperatorTypes.EXPRS),
    INJECT_MEMBERS(PhysicalExprOperator.class),
    FIRST(PhysicalExprOperator.class),
    LENGTH(PhysicalExprOperator.class),
    SINGLETON(PhysicalExprOperator.class),
    SERIALIZE(PhysicalExprOperator.class, PhysicalExprOperator.class),
    DESERIALIZE(PhysicalExprOperator.class, PhysicalExprOperator.class);

    private final ArgumentsTypeChecker checker;


    PhysicalExprOperator(Object... types) {
        checker = TypeCheckers.make(this, types);
    }


    @Override
    public void checkArguments(Object... args) {
        checker.check(args);
    }


    public boolean asyncFor(OperatorNode<PhysicalExprOperator> node) {
        final boolean[] result = new boolean[]{false};
        node.visitNode(new OperatorNodeVisitor() {
            @Override
            public void visit(Object arg) {

            }

            @Override
            public <T extends Operator> boolean enter(OperatorNode<T> node) {
                return !result[0];
            }

            @Override
            public <T extends Operator> void exit(OperatorNode<T> node) {
                if (node.getOperator() instanceof PhysicalExprOperator) {
                    OperatorNode<PhysicalExprOperator> expr = (OperatorNode<PhysicalExprOperator>) node;
                    switch (expr.getOperator()) {
                        case ROOT_CONTEXT:
                        case TIMEOUT_MAX:
                        case TRACE_CONTEXT:
                        case END_CONTEXT:
                        case NEW:
                        case AND:
                        case OR:
                        case EQ:
                        case FIRST:
                        case SINGLETON:
                        case LENGTH:
                        case NEQ:
                        case BOOLEAN_COMPARE:
                        case IN:
                        case IS_NULL:
                        case MATCHES:
                        case CONTAINS:
                        case BINARY_MATH:
                        case COMPARE:
                        case MULTICOMPARE:
                        case COALESCE:
                        case IF:
                        case NEGATE:
                        case NOT:
                        case RECORD:
                        case PROJECT:
                        case RECORD_AS:
                        case ARRAY:
                        case INDEX:
                        case PROPREF:
                        case CALL:
                        case INVOKE:
                        case VALUE:
                        case LOCAL:
                        case CONSTANT:
                        case NULL:
                        case FOREACH:
                        case INJECT_MEMBERS:
                        case CONCAT:
                        case GENERATE_KEYS:
                        case BOOL:

                        case CATCH:
                        case TIMEOUT_GUARD:
                        case CAST:
                        case STREAM_EXECUTE:
                        case STREAM_CREATE:
                        case WITH_CONTEXT:
                        case CURRENT_CONTEXT:
                        case SERIALIZE:
                        case DESERIALIZE:
                            return;
                        case ENFORCE_TIMEOUT:
                        case STREAM_COMPLETE:
                        case ASYNC_INVOKE:
                            result[0] = true;
                            break;
                        default:
                            throw new IllegalArgumentException("unknown PhysicalExprOperator: " + node.getOperator());
                    }
                }
            }
        });
        return result[0];
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
