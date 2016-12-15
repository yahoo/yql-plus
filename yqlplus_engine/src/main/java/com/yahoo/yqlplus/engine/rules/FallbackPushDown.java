/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.operator.OperatorVisitor;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import javax.annotation.Nullable;
import java.util.Deque;
import java.util.EnumSet;
import java.util.Set;

/**
 * Push transform operators through a FALLBACK operator.
 */
public class FallbackPushDown extends LogicalOperatorTransform {
    private static final EnumSet<SequenceOperator> PUSH_OPERATORS =
            EnumSet.of(SequenceOperator.SORT,
                    SequenceOperator.FILTER,
                    SequenceOperator.LIMIT,
                    SequenceOperator.OFFSET,
                    SequenceOperator.SLICE,
                    SequenceOperator.TIMEOUT,
                    SequenceOperator.PROJECT);

    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        // find chains of transforms with multiple FILTER nodes and merge them into one filter node
        if (!PUSH_OPERATORS.contains(node.getOperator())) {
            return super.visitSequenceOperator(node);
        }
        return visitChain(new Chain(), node);
    }

    private static class Chain {
        Deque<OperatorNode<SequenceOperator>> operations = Lists.newLinkedList();
    }

    private Object[] replace(Object[] input, int i, Object value) {
        Object[] result = new Object[input.length];
        System.arraycopy(input, 0, result, 0, input.length);
        result[i] = value;
        return result;
    }

    private Set<String> findSources(final OperatorNode<SequenceOperator> node) {
        final Set<String> sources = Sets.newHashSet();
        node.visit(new OperatorVisitor() {
            @Override
            public <T extends Operator> boolean enter(OperatorNode<T> node) {
                if (node.getOperator() instanceof SequenceOperator) {
                    OperatorNode<SequenceOperator> op = (OperatorNode<SequenceOperator>) node;
                    if (op.getAnnotation("alias") != null) {
                        sources.add((String) node.getAnnotation("alias"));
                        return false;
                    }
                    return true;
                }
                return false;
            }

            @Override
            public <T extends Operator> void exit(OperatorNode<T> node) {
            }
        });
        return sources;
    }


    private OperatorNode<SequenceOperator> replaceArgument(OperatorNode<SequenceOperator> current, OperatorNode<SequenceOperator> input, int i, OperatorNode<SequenceOperator> replacement) {
        // if current has an alias, and we are pushing a node "through" it, we need to mutate any field references we push through
        // an alias operator will "flatten" which may mean we have to disambiguate which field of the underlying input
        // is intended
        // .. which we can't do without the schemas, which we do not have at this point
        // .. feh
        // OK, make it work for the single-visible-alias case and think about how to address the general case
        OperatorNode<SequenceOperator> result = OperatorNode.create(input.getLocation(), input.getAnnotations(), input.getOperator(), replace(input.getArguments(), i, replacement));
        if (current.getAnnotation("alias") != null) {
            Set<String> visible = findSources(result);
            //noinspection StatementWithEmptyBody
            if (visible.isEmpty()) {
                // is this even possible?
            } else if (visible.size() > 1) {
                throw new ProgramCompileException("Pushing transforms below FALLBACK operators with multiple visible aliases not yet supported");
            } else { // visible.size() == 1
                final String oldAlias = (String) current.getAnnotation("alias");
                final String newAlias = Iterables.get(visible, 0);
                result = result.transform(new Function<Object, Object>() {
                    @Nullable
                    @Override
                    public Object apply(@Nullable Object input) {
                        if (input instanceof OperatorNode) {
                            if (ExpressionOperator.IS.apply((OperatorNode<? extends Operator>) input)) {
                                OperatorNode<ExpressionOperator> expr = (OperatorNode<ExpressionOperator>) input;
                                if (expr.getOperator() == ExpressionOperator.READ_FIELD || expr.getOperator() == ExpressionOperator.READ_RECORD) {
                                    String refAlias = expr.getArgument(0);
                                    if (refAlias.equals(oldAlias)) {
                                        return OperatorNode.create(expr.getLocation(), expr.getAnnotations(), expr.getOperator(), replace(expr.getArguments(), 0, newAlias));
                                    }
                                }
                            }
                            return ((OperatorNode<? extends Operator>) input).transform(this);
                        } else {
                            return input;
                        }
                    }
                });

            }
        }
        return result;
    }

    private OperatorNode<SequenceOperator> visitChain(Chain chain, OperatorNode<SequenceOperator> current) {
        if (PUSH_OPERATORS.contains(current.getOperator())) {
            chain.operations.addFirst(current);
            return visitChain(chain, (OperatorNode<SequenceOperator>) current.getArgument(0));
        } else if (current.getOperator() == SequenceOperator.FALLBACK) {
            // we want to take the sequence of chained operators between top and current, apply them to the primary and fallback sides
            // (mutating field alias names as needed), and then return a new FALLBACK node with the new primary/fallback.

            // if there's any sorting, we'd like to do it AFTER we filter
            // 'current' contains what will be the target of the new filter (as it is neither FILTER or SORT)
            // we know 'top' is a FILTER because it started as such
            // so, walk between top and current, accumulating filters and sorting

            // well, feh, we need a way to express "search for this field in the current row"
            // READ_FIELD <field>
            // then we can rewrite field references to aliases that we push through to that when it's ambiguous


            OperatorNode<SequenceOperator> primary = (OperatorNode<SequenceOperator>) current.getArgument(0);
            OperatorNode<SequenceOperator> fallback = (OperatorNode<SequenceOperator>) current.getArgument(1);
            while (!chain.operations.isEmpty()) {
                OperatorNode<SequenceOperator> op = chain.operations.removeFirst();
                // clone op and push it down each side
                switch (op.getOperator()) {
                    case SORT:
                    case FILTER:
                    case SLICE:
                    case TIMEOUT:
                    case PROJECT:
                        primary = replaceArgument(current, op, 0, primary);
                        fallback = replaceArgument(current, op, 0, fallback);
                        break;
                    default:
                        throw new IllegalStateException("should never happen - pushed operator not in PUSH_OPERATOR set");
                }
            }
            primary = super.visitSequenceOperator(primary);
            fallback = super.visitSequenceOperator(fallback);
            return OperatorNode.create(current.getLocation(), current.getAnnotations(), SequenceOperator.FALLBACK, primary, fallback);
        } else {
            // we hit an operator we can't push through and that isn't FALLBACK -- there's no chain here
            return super.visitSequenceOperator(chain.operations.getLast());
        }
    }
}
