/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Turn a MULTISOURCE into a MERGE of SCANs
 */
public class ExpandMultisource extends LogicalOperatorTransform {
    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> node) {
        if (node.getOperator() != SequenceOperator.MULTISOURCE) {
            return super.visitSequenceOperator(node);
        }
        List<List<String>> sourceNames = node.getArgument(0);
        List<OperatorNode<SequenceOperator>> scans = Lists.newArrayList();
        for (List<String> sourceName : sourceNames) {
            scans.add(OperatorNode.create(SequenceOperator.SCAN, sourceName, ImmutableList.of()));
        }
        OperatorNode<SequenceOperator> merge = OperatorNode.create(node.getLocation(), node.getAnnotations(), SequenceOperator.MERGE, scans);
        return super.visitSequenceOperator(merge);
    }
}
