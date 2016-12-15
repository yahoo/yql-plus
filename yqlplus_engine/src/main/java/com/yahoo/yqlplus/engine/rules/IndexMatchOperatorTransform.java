/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Transform indexable predicate matches into a recognizable form.
 */
public class IndexMatchOperatorTransform extends LogicalOperatorTransform {
    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        // find chains of transforms with multiple FILTER nodes and merge them into one filter node

        if (node.getOperator() != SequenceOperator.FILTER) {
            return super.visitSequenceOperator(node);
        }

        return super.visitSequenceOperator(transformFilter(node));
    }

    private OperatorNode<SequenceOperator> transformFilter(OperatorNode<SequenceOperator> node) {
        OperatorNode<SequenceOperator> target = node.getArgument(0);
        OperatorNode<ExpressionOperator> expr = node.getArgument(1);
        OperatorNode<ExpressionOperator> transformed = (OperatorNode<ExpressionOperator>) new PushAndTransform().apply(expr);
        boolean changed = false;
        if (transformed.getOperator() == ExpressionOperator.OR) {
            // transform each clause
            List<OperatorNode<ExpressionOperator>> input = transformed.getArgument(0);
            List<OperatorNode<ExpressionOperator>> clauses = Lists.newArrayList();
            for (OperatorNode<ExpressionOperator> clause : input) {
                clauses.add(transformClause(clause));
            }
            if (!input.equals(clauses)) {
                transformed = OperatorNode.create(transformed.getLocation(), transformed.getAnnotations(), ExpressionOperator.OR, clauses);
                changed = true;
            }
        } else {
            OperatorNode<ExpressionOperator> result = transformClause(transformed);
            if (!result.equals(transformed)) {
                transformed = result;
                changed = true;
            }
        }
        if (changed) {
            return OperatorNode.create(node.getLocation(), node.getAnnotations(), SequenceOperator.FILTER, target, transformed);
        } else {
            return node;
        }

    }

    private OperatorNode<ExpressionOperator> transformCanonical(OperatorNode<ExpressionOperator> candidate) {
        // TODO: should verify that the "other" side does not depend on the zip-matched value, since that would mean this can't be an index match
        if (candidate.getOperator() == ExpressionOperator.EQ) {
            OperatorNode<ExpressionOperator> left = candidate.getArgument(0);
            OperatorNode<ExpressionOperator> right = candidate.getArgument(1);
            boolean leftRead = left.getOperator() == ExpressionOperator.READ_FIELD;
            boolean rightRead = right.getOperator() == ExpressionOperator.READ_FIELD;
            if (leftRead && !rightRead) {
                return OperatorNode.create(candidate.getLocation(), candidate.getAnnotations(), ExpressionOperator.EQ, left, right);
            } else if (rightRead && !leftRead) {
                return OperatorNode.create(candidate.getLocation(), candidate.getAnnotations(), ExpressionOperator.EQ, right, left);
            }
        } else if (candidate.getOperator() == ExpressionOperator.IN) {
            OperatorNode<ExpressionOperator> left = candidate.getArgument(0);
            OperatorNode<ExpressionOperator> right = candidate.getArgument(1);
            if (left.getOperator() == ExpressionOperator.READ_FIELD) {
                return OperatorNode.create(candidate.getLocation(), candidate.getAnnotations(), ExpressionOperator.IN, left, right);
            }
        }
        return null;
    }

    private OperatorNode<ExpressionOperator> transformClause(OperatorNode<ExpressionOperator> clause) {
        // transform any ANDed combinations of (EQ {READ_COLUMN} expr), (EQ expr {READ_COLUMN}) and (IN {READ_COLUMN} iterable-expr)
        // into a ZIP_MATCH operator
        // combine any ZIP_MATCH operators, too!
        if (clause.getOperator() != ExpressionOperator.AND) {
            return clause;
        }
        List<OperatorNode<ExpressionOperator>> clauses = clause.getArgument(0);
        List<OperatorNode<ExpressionOperator>> candidates = Lists.newArrayList();
        List<OperatorNode<ExpressionOperator>> other = Lists.newArrayList();
        for (OperatorNode<ExpressionOperator> candidate : clauses) {
            OperatorNode<ExpressionOperator> zip = transformCanonical(candidate);
            if (zip != null) {
                candidates.add(zip);
            } else {
                other.add(candidate);
            }
        }
        if (!candidates.isEmpty()) {
            OperatorNode<ExpressionOperator> canonicalMatch = OperatorNode.create(clause.getLocation(), clause.getAnnotations(), ExpressionOperator.AND, candidates);
            canonicalMatch.putAnnotation("index", true);
            if (!other.isEmpty()) {
                other.add(0, canonicalMatch);
                return OperatorNode.create(ExpressionOperator.AND, other);
            } else {
                return canonicalMatch;
            }
        }
        return clause;
    }
}
