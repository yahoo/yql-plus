package com.yahoo.yqlplus.engine;

import com.yahoo.yqlplus.engine.compiler.code.EngineValueTypeAdapter;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;

import java.util.List;

public interface CompileContext {
    OperatorValue evaluateValue(OperatorNode<PhysicalExprOperator> input);

    StreamValue execute(OperatorNode<SequenceOperator> query);

    StreamValue executeSource(OperatorNode<SequenceOperator> sequence, Sourcer source);

    TypeWidget adapt(java.lang.reflect.Type type, boolean nullable);

    EngineValueTypeAdapter getValueTypeAdapter();

    OperatorNode<PhysicalExprOperator> evaluate(OperatorNode<ExpressionOperator> value);

    OperatorNode<PhysicalExprOperator> evaluateInRowContext(OperatorNode<ExpressionOperator> value, OperatorNode<PhysicalExprOperator> row);

    List<OperatorNode<PhysicalExprOperator>> evaluateAll(List<OperatorNode<ExpressionOperator>> values);

    List<OperatorNode<PhysicalExprOperator>> evaluateAllInRowContext(List<OperatorNode<ExpressionOperator>> values, OperatorNode<PhysicalExprOperator> row);

    OperatorNode<PhysicalExprOperator> computeExpr(OperatorNode<PhysicalExprOperator> op);

    List<OperatorNode<PhysicalExprOperator>> computeExprs(List<OperatorNode<PhysicalExprOperator>> op);

    List<OperatorNode<PhysicalExprOperator>> evaluateList(List<OperatorNode<ExpressionOperator>> args);

    OperatorNode<PhysicalExprOperator> getContextExpr();

    OperatorNode<PhysicalExprOperator> constant(Object constant);
}
