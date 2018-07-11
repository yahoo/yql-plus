/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.source;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.engine.compiler.code.GambitCreator;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.IndexedSourceType;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.StreamOperator;
import com.yahoo.yqlplus.operator.StreamValue;

import java.util.List;

public class IndexedMethod {
    protected final TypeWidget rowType;
    protected final IndexDescriptor indexDescriptor;
    protected final boolean async;
    protected final boolean singleton;
    protected final QueryType type;
    protected final GambitCreator.Invocable invoker;

    public IndexedMethod(TypeWidget rowType, GambitCreator.Invocable invoker, QueryType indexType, boolean singleton, boolean async, IndexDescriptor descriptor) {
        this.rowType = rowType;
        this.invoker = invoker;
        this.type = indexType;
        this.singleton = singleton;
        this.async = async;
        this.indexDescriptor = descriptor;
    }

    protected StreamValue createKeyCursor(ContextPlanner planner, Location location, List<IndexedSourceType.IndexQuery> todo) {
        if (todo.size() == 1) {
            return todo.get(0).keyCursor(planner);
        } else {
            List<StreamValue> cursors = Lists.newArrayListWithExpectedSize(todo.size());
            for (IndexedSourceType.IndexQuery q : todo) {
                cursors.add(q.keyCursor(planner));
            }
            StreamValue val = StreamValue.merge(planner, cursors);
            val.add(Location.NONE, StreamOperator.DISTINCT);
            return val;
        }
    }

    protected OperatorNode<PhysicalExprOperator> createInvocation(Location location, OperatorNode<PhysicalExprOperator> source, ContextPlanner planner, OperatorNode<PhysicalExprOperator> key, List<OperatorNode<PhysicalExprOperator>> moreArguments) {
        List<OperatorNode<PhysicalExprOperator>> callArgs = Lists.newArrayListWithExpectedSize(2);
        callArgs.add(source);
        callArgs.add(OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT));
        if (indexDescriptor != null) {
            callArgs.add(key);
        }
        callArgs.addAll(moreArguments);
        return OperatorNode.create(location,
                PhysicalExprOperator.INVOKE,
                invoker,
                callArgs);
    }

    enum QueryType {
        SINGLE,
        BATCH,
        SCAN
    }
}
