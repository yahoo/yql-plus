package com.yahoo.yqlplus.engine;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;
import java.util.Set;

public class ChainState {
    private OperatorNode<ExpressionOperator> filter;
    private List<OperatorNode<SequenceOperator>> transforms = Lists.newArrayList();
    private Set<OperatorNode<SequenceOperator>> handled = Sets.newIdentityHashSet();
    private boolean filtered = false;

    public OperatorNode<ExpressionOperator> getFilter() {
        return filter;
    }

    public void setFilterHandled(boolean filtered) {
        this.filtered = filtered;
    }

    public void setFilter(OperatorNode<ExpressionOperator> filter) {
        this.filter = filter;
    }

    public List<OperatorNode<SequenceOperator>> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<OperatorNode<SequenceOperator>> transforms) {
        this.transforms = transforms;
    }

    public Set<OperatorNode<SequenceOperator>> getHandled() {
        return handled;
    }

    public void setHandled(Set<OperatorNode<SequenceOperator>> handled) {
        this.handled = handled;
    }

    public boolean isFiltered() {
        return filtered;
    }
}
