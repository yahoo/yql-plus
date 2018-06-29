/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class MergeFilters extends LogicalOperatorTransform {
    private static final EnumSet<SequenceOperator> MERGE_THROUGH = EnumSet.of(SequenceOperator.SORT, SequenceOperator.FILTER);

    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        // find chains of transforms with multiple FILTER nodes and merge them into one filter node

        if (node.getOperator() != SequenceOperator.FILTER) {
            return super.visitSequenceOperator(node);
        }
        return visitChain(node, node);
    }

    private OperatorNode<SequenceOperator> visitChain(OperatorNode<SequenceOperator> top, OperatorNode<SequenceOperator> current) {
        if (MERGE_THROUGH.contains(current.getOperator())) {
            return visitChain(top, current.getArgument(0));
        } else if (top == current) {
            return top;
        } else {
            // if there's any sorting, we'd like to do it AFTER we filter
            // 'current' contains what will be the target of the new filter (as it is neither FILTER or SORT)
            // we know 'top' is a FILTER because it started as such
            // so, walk between top and current, accumulating filters and sorting
            Deque<OperatorNode<SequenceOperator>> sorts = Lists.newLinkedList();
            List<OperatorNode<ExpressionOperator>> filters = Lists.newArrayList();
            Map<String, Object> annotations = Maps.newLinkedHashMap();
            OperatorNode<SequenceOperator> n = top;
            while (n != current) {
                if (n.getOperator() == SequenceOperator.FILTER) {
                    annotations.putAll(n.getAnnotations());
                    filters.add(super.visitExpr(n.getArgument(1)));
                    n = n.getArgument(0);
                } else if (n.getOperator() == SequenceOperator.SORT) {
                    sorts.addFirst(n);
                }
            }
            current = super.visitSequenceOperator(current);
            OperatorNode<ExpressionOperator> filter = createFilter(filters);
            OperatorNode<SequenceOperator> filtered = OperatorNode.create(top.getLocation(), annotations, SequenceOperator.FILTER, current, filter);
            while (!sorts.isEmpty()) {
                OperatorNode<SequenceOperator> sort = sorts.removeLast();
                filtered = OperatorNode.create(sort.getLocation(), sort.getAnnotations(), sort.getOperator(), filtered, sort.getArgument(1));
            }
            return filtered;
        }
    }

    private OperatorNode<ExpressionOperator> createFilter(List<OperatorNode<ExpressionOperator>> filters) {
        if (filters.size() == 1) {
            return filters.get(0);
        } else {
            return OperatorNode.create(ExpressionOperator.AND, filters);
        }
    }
}
