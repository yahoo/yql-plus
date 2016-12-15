/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;
import java.util.Map;

class IndexStrategy {
    final IndexKey index;
    final IndexDescriptor descriptor;
    Map<String, OperatorNode<ExpressionOperator>> indexFilter;
    OperatorNode<ExpressionOperator> filter;
    List<String> joinColumns;

    IndexStrategy(IndexKey index, IndexDescriptor descriptor) {
        this.index = index;
        this.descriptor = descriptor;
    }
}
