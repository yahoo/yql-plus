package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;
import java.util.Set;

public class ChainState {
    OperatorNode<ExpressionOperator> filter;
    List<OperatorNode<SequenceOperator>> transforms = Lists.newArrayList();
    Set<OperatorNode<SequenceOperator>> handled = Sets.newIdentityHashSet();

    boolean filtered = false;

    public OperatorNode<ExpressionOperator> getFilter() {
        return filter;
    }

    public void setFilterHandled(boolean filtered) {
        this.filtered = filtered;
    }

}
