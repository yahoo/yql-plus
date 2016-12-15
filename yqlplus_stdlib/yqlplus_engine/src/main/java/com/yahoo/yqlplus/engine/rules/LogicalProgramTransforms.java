/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.logical.StatementOperatorTransform;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Transforms applied to the entire program.
 */
public class LogicalProgramTransforms {
    private static final LogicalTransforms SEQUENCE_TRANSFORMS = new LogicalTransforms();

    private static class SequenceTransformer extends StatementOperatorTransform {
        private ViewRegistry viewRegistry;

        private SequenceTransformer(ViewRegistry viewRegistry) {
            this.viewRegistry = viewRegistry;
        }

        @Override
        public OperatorNode<StatementOperator> visitStatement(OperatorNode<StatementOperator> stmt) {
            switch (stmt.getOperator()) {
                case EXECUTE: {
                    OperatorNode<SequenceOperator> query = stmt.getArgument(0);
                    String variableName = stmt.getArgument(1);
                    OperatorNode<SequenceOperator> transformedQuery = SEQUENCE_TRANSFORMS.apply(query, viewRegistry);
                    if (transformedQuery != query) {
                        stmt = OperatorNode.create(stmt.getLocation(), stmt.getAnnotations(), stmt.getOperator(), transformedQuery, variableName);
                    }
                }
            }
            return stmt.transform(this);
        }
    }

    public OperatorNode<StatementOperator> apply(OperatorNode<StatementOperator> program, ViewRegistry scope) {
        List<StatementOperatorTransform> transforms = Lists.newArrayList(
                new ReplaceSubqueryFilters(),
                new SequenceTransformer(scope)
        );
        for (StatementOperatorTransform transform : transforms) {
            program = transform.visitStatement(program);
        }
        return program;
    }
}
