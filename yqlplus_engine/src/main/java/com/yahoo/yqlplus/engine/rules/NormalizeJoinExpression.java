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
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.List;
import java.util.Set;

/**
 * Normalize JOIN expressions so they are all LEFT = RIGHT
 */
public class NormalizeJoinExpression extends LogicalOperatorTransform {
    private Set<String> findSources(final OperatorNode<SequenceOperator> node) {
        final Set<String> sources = Sets.newHashSet();
        node.visit(new OperatorVisitor() {
            @Override
            public <T extends Operator> boolean enter(OperatorNode<T> node) {
                return node.getOperator() instanceof SequenceOperator;
            }

            @Override
            public <T extends Operator> void exit(OperatorNode<T> node) {
                if (node.getOperator() instanceof SequenceOperator) {
                    if (node.getAnnotation("alias") != null) {
                        sources.add((String) node.getAnnotation("alias"));
                    }
                }
            }
        });
        return sources;
    }

    private Set<String> findReferencedSources(final OperatorNode<ExpressionOperator> node) {
        final Set<String> sources = Sets.newHashSet();
        node.visit(new OperatorVisitor() {
            @Override
            public <T extends Operator> boolean enter(OperatorNode<T> node) {
                return node.getOperator() instanceof ExpressionOperator;
            }

            @Override
            public <T extends Operator> void exit(OperatorNode<T> node) {
                if (node.getOperator() instanceof ExpressionOperator && (node.getOperator() == ExpressionOperator.READ_RECORD || node.getOperator() == ExpressionOperator.READ_FIELD)) {
                    sources.add(node.getArgument(0));
                }
            }
        });
        return sources;
    }

    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> target) {
        // We only operate on JOIN and LEFT_JOIN operations.
        if (target.getOperator() != SequenceOperator.JOIN && target.getOperator() != SequenceOperator.LEFT_JOIN) {
            return super.visitSequenceOperator(target);
        }
        OperatorNode<SequenceOperator> leftSide = target.getArgument(0);
        OperatorNode<SequenceOperator> rightSide = target.getArgument(1);
        OperatorNode<ExpressionOperator> joinExpr = target.getArgument(2);
        Set<String> leftSources = findSources(leftSide);
        Set<String> rightSources = findSources(rightSide);
        OperatorNode<ExpressionOperator> newJoinExpr = normalizeJoinClause(leftSources, rightSources, joinExpr);
        if (joinExpr != newJoinExpr) {
            return super.visitSequenceOperator(OperatorNode.create(target.getLocation(), target.getAnnotations(), target.getOperator(), leftSide, rightSide, newJoinExpr));
        }

        return super.visitSequenceOperator(target);
    }

    private OperatorNode<ExpressionOperator> normalizeJoinClause(Set<String> leftSources, Set<String> rightSources, OperatorNode<ExpressionOperator> joinExpr) {
        switch (joinExpr.getOperator()) {
            case AND: {
                List<OperatorNode<ExpressionOperator>> clauses = joinExpr.getArgument(0);
                List<OperatorNode<ExpressionOperator>> newClauses = Lists.newArrayListWithExpectedSize(clauses.size());
                boolean hasNew = false;
                for (OperatorNode<ExpressionOperator> clause : clauses) {
                    OperatorNode<ExpressionOperator> newClause = normalizeJoinClause(leftSources, rightSources, clause);
                    if (newClause != clause) {
                        hasNew = true;
                    }
                    newClauses.add(newClause);
                }
                if (hasNew) {
                    return OperatorNode.create(joinExpr.getLocation(), joinExpr.getAnnotations(), joinExpr.getOperator(), newClauses);
                }
                return joinExpr;
            }
            case EQ: {
                OperatorNode<ExpressionOperator> leftExpr = joinExpr.getArgument(0);
                OperatorNode<ExpressionOperator> rightExpr = joinExpr.getArgument(1);
                Set<String> leftReferenced = findReferencedSources(leftExpr);
                Set<String> rightReferenced = findReferencedSources(rightExpr);
                boolean ll = !Sets.intersection(leftSources, leftReferenced).isEmpty();
                boolean lr = !Sets.intersection(leftSources, rightReferenced).isEmpty();
                boolean rl = !Sets.intersection(rightSources, leftReferenced).isEmpty();
                boolean rr = !Sets.intersection(rightSources, rightReferenced).isEmpty();
                // ll - left expr references left sources
                // lr - left expr references right sources
                // rl - right expr references left sources
                // rr - right expr references right sources
                // verify neither expr references BOTH sides
                if (ll && lr) {
                    throw new ProgramCompileException(joinExpr.getLocation(), "JOIN expression equality LEFT side references BOTH sides of JOIN");
                } else if (rl && rr) {
                    throw new ProgramCompileException(joinExpr.getLocation(), "JOIN expression equality RIGHT side references BOTH sides of JOIN");
                } else if (!(ll || lr)) {
                    throw new ProgramCompileException(joinExpr.getLocation(), "JOIN expression equality LEFT side references NEITHER side of JOIN");
                } else if (!(rl || rr)) {
                    throw new ProgramCompileException(joinExpr.getLocation(), "JOIN expression equality RIGHT side references NEITHER side of JOIN");
                }
                // normalize ordering so left side of EQ refers to left side of join
                if (lr) {
                    assert rl : "lr without rl - if left side references right sources, then visa versa must be true";
                    return OperatorNode.create(joinExpr.getLocation(), joinExpr.getAnnotations(), joinExpr.getOperator(), rightExpr, leftExpr);
                }
                return joinExpr;
            }
            default:
                throw new ProgramCompileException(joinExpr.getLocation(), "Only EQ is a supported JOIN expression operator at this time (not %s)", joinExpr.getOperator());
        }
    }
}

