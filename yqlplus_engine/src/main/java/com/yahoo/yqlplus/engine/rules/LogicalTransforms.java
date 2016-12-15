/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Perform the logical transforms in the correct order.
 */
@Singleton
public class LogicalTransforms {
    public OperatorNode<SequenceOperator> apply(OperatorNode<SequenceOperator> query, ViewRegistry scope) {
        List<LogicalOperatorTransform> transforms = Lists.newArrayList(
                new ExpandMultisource(),
                new ExpandViews(scope),
                new FallbackPushDown(),
                new JoinFilterPushDown(),
                new NormalizeJoinExpression(),
                new MergeFilters(),
                new ReadFieldAliasAnnotate()
        );
        for (LogicalOperatorTransform transform : transforms) {
            query = transform.visitSequenceOperator(query);
        }
        return query;
    }
}
