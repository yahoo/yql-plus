package com.yahoo.yqlplus.engine;

import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.operator.StreamValue;

public interface Sourcer {
    StreamValue execute(ContextPlanner context, ChainState state, OperatorNode<SequenceOperator> query);
}
