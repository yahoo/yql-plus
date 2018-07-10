package com.yahoo.yqlplus.engine.source;

import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.SourceType;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.StreamValue;

import java.util.List;
import java.util.function.Supplier;

public class SourceAdapter implements SourceType {
    private final String moduleName;
    private final Class<?> clazz;
    private final Supplier<?> supplier;
    private OperatorNode<PhysicalExprOperator> module;

    public SourceAdapter(String moduleName, Class<?> clazz, Supplier<?> module) {
        this.moduleName = moduleName;
        this.clazz = clazz;
        this.supplier = module;
    }

    @Override
    public StreamValue plan(ContextPlanner planner, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        int count = args.size();

        return null;
    }

    @Override
    public StreamValue join(ContextPlanner planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> right, OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        int count = args.size();
        return null;
    }
}
