package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.operator.*;

import java.util.List;
import java.util.Map;

public class IndexQuery {
    public IndexKey index;
    public Map<String, OperatorNode<PhysicalExprOperator>> keyValues = Maps.newLinkedHashMap();
    public OperatorNode<ExpressionOperator> filter;
    public OperatorNode<FunctionOperator> filterPredicate;
    public boolean handledFilter;
    public StreamValue joinKeyStream;
    public List<String> joinKeys;

    public StreamValue keyCursor(ContextPlanner planner) {
        // TODO: how to handle joinKeyStream along with this key sequence?
        // if only joinKeyStream, use it
        // else
        // maybe CROSS the joinKeyStream with the GENERATE_KEYS?
        if(joinKeys == null || joinKeys.isEmpty()) {
            return prepareKeyStream(planner);
        } else if(keyValues.isEmpty()) {
            return joinKeyStream;
        } else {
            StreamValue input = prepareKeyStream(planner);
            List<OperatorNode<PhysicalProjectOperator>> projection = Lists.newArrayList();
            projection.add(OperatorNode.create(PhysicalProjectOperator.MERGE, OperatorNode.create(PhysicalExprOperator.LOCAL, "$left")));
            projection.add(OperatorNode.create(PhysicalProjectOperator.MERGE, OperatorNode.create(PhysicalExprOperator.LOCAL, "$right")));
            OperatorNode<PhysicalExprOperator> proj = OperatorNode.create(PhysicalExprOperator.PROJECT, projection);
            input.add(Location.NONE, StreamOperator.CROSS, joinKeyStream.materializeValue(),
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$left", "$right"),
                            OperatorNode.create(PhysicalExprOperator.SINGLETON,
                                    proj)));
            input.add(Location.NONE, StreamOperator.DISTINCT);
            return input;
        }
    }

    private StreamValue prepareKeyStream(ContextPlanner planner) {
        List<String> keys = index.columnOrder;
        List<OperatorNode<PhysicalExprOperator>> valueLists = Lists.newArrayList();
        for (String key : keys) {
            if(keyValues.containsKey(key)) {
                valueLists.add(keyValues.get(key));
            }
        }
        return StreamValue.iterate(planner, OperatorNode.create(PhysicalExprOperator.GENERATE_KEYS, keys, valueLists));
    }
}
