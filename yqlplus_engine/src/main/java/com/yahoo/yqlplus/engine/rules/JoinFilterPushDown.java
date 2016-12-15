/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.operator.OperatorVisitor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JoinFilterPushDown extends LogicalOperatorTransform {
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

    private Set<String> findReferencedSources(final OperatorNode<ExpressionOperator> node) {
        final Set<String> sources = Sets.newHashSet();
        node.visit(new OperatorVisitor() {
            @Override
            public <T extends Operator> boolean enter(OperatorNode<T> node) {
                if (node.getOperator() instanceof ExpressionOperator) {
                    return true;
                }
                return false;
            }

            @Override
            public <T extends Operator> void exit(OperatorNode<T> node) {
                if (node.getOperator() instanceof ExpressionOperator && (node.getOperator() == ExpressionOperator.READ_RECORD || node.getOperator() == ExpressionOperator.READ_FIELD)) {
                    sources.add((String) node.getArgument(0));
                }
            }
        });
        return sources;
    }

    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        // if this is a FILTER AND it contains a JOIN (perhaps with some other transforms in the way)
        //   AND the filter contains only references to the left side of the join
        if (node.getOperator() != SequenceOperator.FILTER) {
            return super.visitSequenceOperator(node);
        }
        // we have a FILTER, see if there's a JOIN underneath
        OperatorNode<SequenceOperator> target = node.getArgument(0);
        OperatorNode<ExpressionOperator> filter = node.getArgument(1);
        // It has to be *directly* underneath due to the way logical operands are constructed by the current parser
        // there may be a stack of JOINs, but we can attack each one in sequence

        if (target.getOperator() == SequenceOperator.JOIN || target.getOperator() == SequenceOperator.LEFT_JOIN) {
            List<OperatorNode<ExpressionOperator>> top = Lists.newArrayList();
            List<OperatorNode<ExpressionOperator>> leftFilter = Lists.newArrayList();
            List<OperatorNode<ExpressionOperator>> rightFilter = Lists.newArrayList();
            OperatorNode<SequenceOperator> leftSide = target.getArgument(0);
            OperatorNode<SequenceOperator> rightSide = target.getArgument(1);
            OperatorNode<ExpressionOperator> joinExpr = target.getArgument(2);
            if (filter.getOperator() == ExpressionOperator.AND) {
                flatten(top, (List<OperatorNode<ExpressionOperator>>) filter.getArgument(0));
            } else {
                top.add(filter);
            }
            Iterator<OperatorNode<ExpressionOperator>> topIterator = top.iterator();
            while (topIterator.hasNext()) {
                OperatorNode<ExpressionOperator> clause = topIterator.next();
                Set<String> left = findSources(leftSide);
                Set<String> right = findSources(rightSide);
                Set<String> referencedFilter = findReferencedSources(clause);
                boolean useLeft = !Sets.intersection(referencedFilter, left).isEmpty();
                boolean useRight = !Sets.intersection(referencedFilter, right).isEmpty();
                if (useLeft && useRight) {
                    // can't do anything
                } else if (useLeft) {
                    leftFilter.add(clause);
                    topIterator.remove();
                } else if (useRight) {
                    rightFilter.add(clause);
                    topIterator.remove();
                }
            }
            OperatorNode<SequenceOperator> result = node;
            if (rightFilter.size() > 0) {
                rightSide = visitSequenceOperator(new MergeFilters().visitSequenceOperator(OperatorNode.create(node.getLocation(), SequenceOperator.FILTER, rightSide, createFilter(rightFilter))));
            }
            if (leftFilter.size() > 0) {
                leftSide = visitSequenceOperator(new MergeFilters().visitSequenceOperator(OperatorNode.create(node.getLocation(), SequenceOperator.FILTER, leftSide, createFilter(leftFilter))));
            }
            if (rightFilter.size() > 0 || leftFilter.size() > 0) {
                result = OperatorNode.create(target.getLocation(), target.getAnnotations(), target.getOperator(),
                        leftSide, rightSide, joinExpr);
                if (top.size() > 0) {
                    result = OperatorNode.create(node.getLocation(), node.getAnnotations(), node.getOperator(), createFilter(top));
                } else {
                    for (Map.Entry<String, Object> e : node.getAnnotations().entrySet()) {
                        result.putAnnotation(e.getKey(), e.getValue());
                    }
                }
            }
            return super.visitSequenceOperator(result);
        }
        return node;
    }

    private void flatten(List<OperatorNode<ExpressionOperator>> top, List<OperatorNode<ExpressionOperator>> argument) {
        for (OperatorNode<ExpressionOperator> op : argument) {
            if (op.getOperator() == ExpressionOperator.AND) {
                List<OperatorNode<ExpressionOperator>> clauses = op.getArgument(0);
                flatten(top, clauses);
            } else {
                top.add(op);
            }
        }
    }

    private OperatorNode<ExpressionOperator> createFilter(List<OperatorNode<ExpressionOperator>> rightFilter) {
        if (rightFilter.size() > 1) {
            return OperatorNode.create(ExpressionOperator.AND, rightFilter);
        } else {
            return rightFilter.get(0);
        }
    }
}
