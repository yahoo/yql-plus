/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.compiler.runtime.ArithmeticOperation;
import com.yahoo.yqlplus.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.compiler.runtime.Like;
import com.yahoo.yqlplus.engine.rules.ReadFieldAliasAnnotate;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DynamicExpressionEvaluator implements Function<OperatorNode<ExpressionOperator>, OperatorNode<PhysicalExprOperator>> {
    private static final ConstantExpressionEvaluator CONSTANT = new ConstantExpressionEvaluator();
    private final DynamicExpressionEnvironment environment;
    private final OperatorNode<PhysicalExprOperator> row;

    public DynamicExpressionEvaluator(DynamicExpressionEnvironment environment, OperatorNode<PhysicalExprOperator> row) {
        this.environment = environment;
        this.row = row;
    }

    public DynamicExpressionEvaluator(DynamicExpressionEnvironment environment) {
        this.environment = environment;
        this.row = null;
    }

    public List<OperatorNode<PhysicalExprOperator>> applyAll(List<OperatorNode<ExpressionOperator>> inputs) {
        List<OperatorNode<PhysicalExprOperator>> out = Lists.newArrayListWithExpectedSize(inputs.size());
        for (OperatorNode<ExpressionOperator> arg : inputs) {
            out.add(apply(arg));
        }
        return out;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> apply(OperatorNode<ExpressionOperator> input) {
        // there may be more efficient ways to do this, but let's see how this works
        try {
            Object result = CONSTANT.apply(input);
            return constant(result);
        } catch (NotConstantExpressionException e) {
            // not constant, fall through to dynamic eval
        }
        switch (input.getOperator()) {
            case AND:
            case OR: {
                List<OperatorNode<ExpressionOperator>> clauses = input.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> output = Lists.newArrayList();
                for (OperatorNode<ExpressionOperator> clause : clauses) {
                    output.add(apply(clause));
                }
                return OperatorNode.create(input.getLocation(), input.getOperator() == ExpressionOperator.AND ? PhysicalExprOperator.AND : PhysicalExprOperator.OR, output);
            }
            case EQ:
            case NEQ:
            case LT:
            case GT:
            case LTEQ:
            case GTEQ:
            case NOT_IN:
            case IN: {
                return compare(input.getLocation(), input.getOperator(),
                        apply(input.getArgument(0)),
                        apply(input.getArgument(1)));
            }
            case LIKE:
            case NOT_LIKE:
            case MATCHES:
            case NOT_MATCHES: {
                // these require a constant pattern
                ConstantExpressionEvaluator patternEval = new ConstantExpressionEvaluator();
                OperatorNode<ExpressionOperator> pattern = input.getArgument(1);
                Object patternValue = patternEval.apply(pattern);
                if (patternValue instanceof String) {
                    if (input.getOperator() == ExpressionOperator.LIKE || input.getOperator() == ExpressionOperator.NOT_LIKE) {
                        patternValue = Like.compileLike((CharSequence) patternValue);
                    } else {
                        patternValue = Pattern.compile((String) patternValue);
                    }
                } else if (!(patternValue instanceof Pattern)) {
                    throw new ProgramCompileException(input.getLocation(), input.getOperator() + " pattern must be constant String or Pattern");
                }
                OperatorNode<PhysicalExprOperator> left = apply(input.getArgument(0));
                OperatorNode<PhysicalExprOperator> right = environment.constant(patternValue);
                return compare(input.getLocation(), input.getOperator(), left, right);
            }
            case IS_NULL:
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.IS_NULL, apply(input.getArgument(0)));
            case IS_NOT_NULL: {
                OperatorNode<PhysicalExprOperator> target = apply(input.getArgument(0));
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.NOT,
                        OperatorNode.create(input.getLocation(), PhysicalExprOperator.IS_NULL, target));
            }
            case ADD:
            case SUB:
            case MULT:
            case DIV:
            case MOD: {
                OperatorNode<PhysicalExprOperator> left = apply(input.getArgument(0));
                OperatorNode<PhysicalExprOperator> right = apply(input.getArgument(1));
                ArithmeticOperation arithmeticOperation = MATH.get(input.getOperator());
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.BINARY_MATH, arithmeticOperation,
                        left, right);
            }
            case NEGATE:
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.NEGATE, apply(input.getArgument(0)));
            case NOT:
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.NOT, apply(input.getArgument(0)));
            case MAP: {
                List<String> keys = input.getArgument(0);
                List<OperatorNode<ExpressionOperator>> values = input.getArgument(1);
                List<OperatorNode<PhysicalExprOperator>> fieldValues = Lists.newArrayList();
                for (int i = 0; i < keys.size(); ++i) {
                    fieldValues.add(apply(values.get(i)));
                }
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.RECORD, keys, fieldValues);
            }
            case ARRAY: {
                List<OperatorNode<ExpressionOperator>> args = input.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> out = Lists.newArrayList();
                for (OperatorNode<ExpressionOperator> arg : args) {
                    out.add(apply(arg));
                }
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.ARRAY, out);
            }
            case INDEX: {
                OperatorNode<PhysicalExprOperator> left = apply(input.getArgument(0));
                OperatorNode<PhysicalExprOperator> right = apply(input.getArgument(1));
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.INDEX, left, right);
            }
            case PROPREF: {
                OperatorNode<PhysicalExprOperator> left = apply(input.getArgument(0));
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.PROPREF, left, input.getArgument(1));
            }
            case LITERAL:
                return constant(input.getArgument(0));
            case NULL:
                throw new ProgramCompileException("Unsupported ExpressionOperator: NULL");
            case VARREF: {
                String name = input.getArgument(0);
                OperatorValue vari = environment.getVariable(name);
                if (vari == null) {
                    throw new ProgramCompileException(input.getLocation(), "Unknown variable '%s'", name);
                }
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.VALUE, vari);
            }
            case READ_RECORD: {
                String alias = input.getArgument(0);
                if (row == null) {
                    if (environment.getVariable(alias) != null) {
                        throw new ProgramCompileException(input.getLocation(), "No available row reference (perhaps meant @%s?)", alias);
                    }
                    throw new ProgramCompileException(input.getLocation(), "No available row reference");
                }
                OperatorNode<PhysicalExprOperator> target = row;
                ReadFieldAliasAnnotate.RowType rowType = ReadFieldAliasAnnotate.RowType.getReadRecordType(input);
                if (rowType.isJoin()) {
                    target = OperatorNode.create(input.getLocation(), PhysicalExprOperator.PROPREF, target, alias);
                }
                return target;
            }
            case READ_FIELD: {
                String alias = input.getArgument(0);
                String field = input.getArgument(1);
                if (row == null) {
                    if (environment.getVariable(alias) != null) {
                        throw new ProgramCompileException(input.getLocation(), "No available row reference (perhaps meant @%s?)", alias);
                    }
                    throw new ProgramCompileException(input.getLocation(), "No available row reference");
                }
                OperatorNode<PhysicalExprOperator> target = row;
                ReadFieldAliasAnnotate.RowType rowType = ReadFieldAliasAnnotate.RowType.getReadRecordType(input);
                if (rowType.isJoin()) {
                    target = OperatorNode.create(input.getLocation(), PhysicalExprOperator.PROPREF, target, alias);
                }
                return OperatorNode.create(input.getLocation(), PhysicalExprOperator.PROPREF, target, field);
            }
            case CALL: {
                if (row != null) {
                    return environment.call(input, row);
                } else {
                    return environment.call(input);
                }
            }
            case READ_MODULE: {
                List<String> path = input.getArgument(0);
                return environment.property(input.getLocation(), path);
            }
            case CONTAINS:
                throw new NotConstantExpressionException(input.getLocation(), "Operator " + input.getOperator() + " is not supported by dynamic expressions");
            case IN_QUERY:
            case NOT_IN_QUERY: {
                throw new ProgramCompileException(input.getLocation(), "%s not supported; was the ReplaceSubqueries program logical transform run?", input.getOperator());
            }
        }
        throw new ProgramCompileException(input.getLocation(), "Unknown expression operator '%s'", input.getOperator());
    }

    public OperatorNode<PhysicalExprOperator> constant(Object value) {
        return environment.constant(value);
    }

    private static final EnumMap<ExpressionOperator, ArithmeticOperation> MATH = new EnumMap<>(ImmutableMap.of(
            ExpressionOperator.ADD, ArithmeticOperation.ADD,
            ExpressionOperator.SUB, ArithmeticOperation.SUB,
            ExpressionOperator.MULT, ArithmeticOperation.MULT,
            ExpressionOperator.DIV, ArithmeticOperation.DIV,
            ExpressionOperator.MOD, ArithmeticOperation.MOD
    ));


    private static final Map<ExpressionOperator, PhysicalExprOperator> BINARY_MAP = Maps.newEnumMap(ImmutableMap.<ExpressionOperator, PhysicalExprOperator>builder()
            .put(ExpressionOperator.EQ, PhysicalExprOperator.EQ)
            .put(ExpressionOperator.NEQ, PhysicalExprOperator.NEQ)
            .put(ExpressionOperator.IN, PhysicalExprOperator.IN)
            .put(ExpressionOperator.MATCHES, PhysicalExprOperator.MATCHES)
            .put(ExpressionOperator.LIKE, PhysicalExprOperator.MATCHES)
            .build());


    private OperatorNode<PhysicalExprOperator> compare(final Location location, ExpressionOperator operator, OperatorNode<PhysicalExprOperator> left, OperatorNode<PhysicalExprOperator> right) {
        if (BINARY_MAP.containsKey(operator)) {
            return OperatorNode.create(location, BINARY_MAP.get(operator), left, right);
        }
        switch (operator) {
            case LT:
                return OperatorNode.create(location, PhysicalExprOperator.BOOLEAN_COMPARE, BinaryComparison.LT,
                        left, right);
            case GT:
                return OperatorNode.create(location, PhysicalExprOperator.BOOLEAN_COMPARE, BinaryComparison.GT,
                        left, right);
            case LTEQ:
                return OperatorNode.create(location, PhysicalExprOperator.BOOLEAN_COMPARE, BinaryComparison.LTEQ,
                        left, right);
            case GTEQ:
                return OperatorNode.create(location, PhysicalExprOperator.BOOLEAN_COMPARE, BinaryComparison.GTEQ,
                        left, right);
            case NOT_LIKE:
            case NOT_MATCHES:
                return OperatorNode.create(location, PhysicalExprOperator.NOT, compare(location, ExpressionOperator.MATCHES, left, right));
            case NOT_IN:
                return OperatorNode.create(location, PhysicalExprOperator.NOT, compare(location, ExpressionOperator.IN, left, right));
        }
        throw new ProgramCompileException(location, "Unknown binary operator '%s'", operator);
    }
}