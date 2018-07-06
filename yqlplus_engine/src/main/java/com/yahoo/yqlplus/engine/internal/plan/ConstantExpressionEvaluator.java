/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.compiler.runtime.*;
import com.yahoo.yqlplus.flow.internal.dynalink.FlowBootstrapper;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class ConstantExpressionEvaluator implements Function<OperatorNode<ExpressionOperator>, Object> {
    private MethodHandle getprop = FlowBootstrapper.publicBootstrap(null, "dyn:getProp",
            MethodType.methodType(Object.class, Object.class, Object.class)).dynamicInvoker();

    private MethodHandle index = FlowBootstrapper.publicBootstrap(null, "dyn:getElem",
            MethodType.methodType(Object.class, Object.class, Object.class)).dynamicInvoker();

    public List<Object> apply(List<OperatorNode<ExpressionOperator>> input) {
        return Lists.newArrayList(Iterables.transform(input, this));
    }

    @Override
    public Object apply(OperatorNode<ExpressionOperator> input) {
        switch (input.getOperator()) {
            case AND: {
                List<OperatorNode<ExpressionOperator>> clauses = input.getArgument(0);
                for (OperatorNode<ExpressionOperator> clause : clauses) {
                    if (!Booleans.toBoolean(apply(clause))) {
                        return false;
                    }
                }
                return true;
            }
            case OR: {
                List<OperatorNode<ExpressionOperator>> clauses = input.getArgument(0);
                for (OperatorNode<ExpressionOperator> clause : clauses) {
                    if (Booleans.toBoolean(apply(clause))) {
                        return true;
                    }
                }
                return false;
            }
            case EQ:
            case NEQ:
            case LT:
            case GT:
            case LTEQ:
            case GTEQ:
            case NOT_IN:
            case LIKE:
            case NOT_LIKE:
            case MATCHES:
            case NOT_MATCHES:
            case IN: {
                Object left = apply((OperatorNode<ExpressionOperator>) input.getArgument(0));
                Object right = apply((OperatorNode<ExpressionOperator>) input.getArgument(1));
                return compare(input.getLocation(), input.getOperator(), left, right);
            }
            case IS_NULL:
                return apply((OperatorNode<ExpressionOperator>) input.getArgument(0)) == null;
            case IS_NOT_NULL:
                return apply((OperatorNode<ExpressionOperator>) input.getArgument(0)) != null;
            case ADD:
            case SUB:
            case MULT:
            case DIV:
            case MOD: {
                Object left = apply((OperatorNode<ExpressionOperator>) input.getArgument(0));
                Object right = apply((OperatorNode<ExpressionOperator>) input.getArgument(1));
                return math(input.getLocation(), input.getOperator(), left, right);
            }
            case NEGATE:
                return Maths.INSTANCE.dynamicNegate(apply((OperatorNode<ExpressionOperator>) input.getArgument(0)));
            case NOT:
                return !Booleans.toBoolean(apply((OperatorNode<ExpressionOperator>) input.getArgument(0)));
            case MAP: {
                List<String> keys = input.getArgument(0);
                List<Object> values = apply((List<OperatorNode<ExpressionOperator>>) input.getArgument(1));
                ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
                for (int i = 0; i < keys.size(); ++i) {
                    if(values.get(i) == null) {
                        throw new NotConstantExpressionException(input.getLocation(), "Operator " + input.getOperator() + " with NULL value is not supported by constant expressions");
                    }
                    map.put(keys.get(i), values.get(i));
                }
                return new RecordMapWrapper(map.build());
            }
            case ARRAY:
                return apply((List<OperatorNode<ExpressionOperator>>) input.getArgument(0));
            case INDEX: {
                Object target = apply((OperatorNode<ExpressionOperator>) input.getArgument(0));
                Object index = apply((OperatorNode<ExpressionOperator>) input.getArgument(1));
                if (target == null || index == null) {
                    return null;
                }
                try {
                    return this.index.invokeExact(target, index);
                } catch (RuntimeException | Error e) {
                    throw e;
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
            }
            case PROPREF: {
                Object target = apply((OperatorNode<ExpressionOperator>) input.getArgument(0));
                Object index = input.getArgument(1);
                if (target == null) {
                    return null;
                }
                try {
                    return this.getprop.invokeExact(target, index);
                } catch (RuntimeException | Error e) {
                    throw e;
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
            }
            case LITERAL:
                return input.getArgument(0);
            case NULL:
                return null;
            case IN_QUERY:
            case CONTAINS:
            case READ_RECORD:
            case READ_FIELD:
            case READ_MODULE:
            case VARREF:
            case CALL:
            case NOT_IN_QUERY:
                throw new NotConstantExpressionException(input.getLocation(), "Operator " + input.getOperator() + " is not supported by constant expressions");
        }
        throw new ProgramCompileException(input.getLocation(), "Unknown expression operator '%s'", input.getOperator());
    }

    private Object math(Location location, ExpressionOperator operator, Object left, Object right) {
        if (left == null || right == null) {
            return null;
        }
        switch (operator) {
            case ADD:
                return Maths.INSTANCE.dynamicMath(ArithmeticOperation.ADD, left, right);
            case SUB:
                return Maths.INSTANCE.dynamicMath(ArithmeticOperation.SUB, left, right);
            case MULT:
                return Maths.INSTANCE.dynamicMath(ArithmeticOperation.MULT, left, right);
            case DIV:
                return Maths.INSTANCE.dynamicMath(ArithmeticOperation.DIV, left, right);
            case MOD:
                return Maths.INSTANCE.dynamicMath(ArithmeticOperation.MOD, left, right);
        }
        throw new ProgramCompileException(location, "Unknown math operator %s", operator);
    }

    private Boolean compare(final Location location, ExpressionOperator operator, final Object left, final Object right) {
        if (left == null || right == null) {
            return null;
        }
        Comparable<Object> comp;
        if (left instanceof Comparable && right instanceof Comparable) {
            comp = (Comparable<Object>) left;
        } else {
            comp = new Comparable<Object>() {
                @Override
                public int compareTo(Object o) {
                    throw new ProgramCompileException(location, "Unable to compare %s and %s", left.getClass().getName(), right.getClass().getName());
                }
            };
        }
        switch (operator) {
            case EQ:
                return left.equals(right);
            case NEQ:
                return !left.equals(right);
            case LT:
                return comp.compareTo(right) < 0;
            case GT:
                return comp.compareTo(right) > 0;
            case LTEQ:
                return comp.compareTo(right) <= 0;
            case GTEQ:
                return comp.compareTo(right) >= 0;
            case NOT_LIKE:
                return negate(compare(location, ExpressionOperator.LIKE, left, right));
            case NOT_MATCHES:
                return negate(compare(location, ExpressionOperator.MATCHES, left, right));
            case NOT_IN:
                return negate(compare(location, ExpressionOperator.IN, left, right));
            case IN: {
                if (right instanceof Collection) {
                    return ((Collection) right).contains(left);
                }
                throw new ProgramCompileException(location, "right side of IN is not a Collection");
            }
            case LIKE: {
                if (right instanceof CharSequence && left instanceof CharSequence) {
                    Pattern pattern = Like.compileLike((CharSequence) right);
                    return pattern.matcher((CharSequence) left).matches();
                }
                throw new ProgramCompileException(location, "Left and right side of LIKE must be strings");
            }
            case MATCHES: {
                Pattern pattern = null;
                if (right instanceof Pattern) {
                    pattern = (Pattern) right;
                } else if (right instanceof String) {
                    pattern = Pattern.compile((String) right);
                }
                if (left instanceof CharSequence && pattern != null) {
                    return pattern.matcher((CharSequence) left).matches();
                }
                throw new ProgramCompileException(location, "Left and right side of MATCHES must be strings");
            }
        }
        throw new ProgramCompileException(location, "Unknown operator '%s'", operator);
    }

    private Boolean negate(Object result) {
        if (result == null) {
            return null;
        }
        return !(Boolean) result;
    }
}
