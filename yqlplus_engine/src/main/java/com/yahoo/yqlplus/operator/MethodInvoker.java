package com.yahoo.yqlplus.operator;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public interface MethodInvoker {
    boolean isStatic();

    default OperatorNode<PhysicalExprOperator> invoke() {
        return invoke(ImmutableList.of());
    }

    default OperatorNode<PhysicalExprOperator> invoke(OperatorNode<PhysicalExprOperator> arg1) {
        return invoke(ImmutableList.of(arg1));
    }

    default OperatorNode<PhysicalExprOperator> invoke(OperatorNode<PhysicalExprOperator> arg1, OperatorNode<PhysicalExprOperator> arg2) {
        return invoke(ImmutableList.of(arg1, arg2));
    }

    default OperatorNode<PhysicalExprOperator> invoke(OperatorNode<PhysicalExprOperator> arg1, OperatorNode<PhysicalExprOperator> arg2, OperatorNode<PhysicalExprOperator> arg3) {
        return invoke(ImmutableList.of(arg1, arg2, arg3));
    }

    default OperatorNode<PhysicalExprOperator> invoke(OperatorNode<PhysicalExprOperator> arg1, OperatorNode<PhysicalExprOperator> arg2, OperatorNode<PhysicalExprOperator> arg3, OperatorNode<PhysicalExprOperator> arg4) {
        return invoke(ImmutableList.of(arg1, arg2, arg3, arg4));
    }

    OperatorNode<PhysicalExprOperator> invoke(List<OperatorNode<PhysicalExprOperator>> args);
}
