/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.index.IndexColumn;
import com.yahoo.yqlplus.engine.internal.plan.ast.ExprScope;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalProjectOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.SinkOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NotNullableTypeWidget;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class IndexedSourceType implements SourceType {
    private final IndexedQueryPlanner indexPlanner;
    private final IndexedQueryPlanner deletePlanner;
    private final IndexedQueryPlanner updatePlanner;

    protected IndexedSourceType(IndexedQueryPlanner indexPlanner, IndexedQueryPlanner deletePlanner, IndexedQueryPlanner updatePlanner) {
        this.indexPlanner = indexPlanner;
        this.deletePlanner = deletePlanner;
        this.updatePlanner = updatePlanner;
    }

    protected StreamValue scan(Location location, final ContextPlanner planner, PlanChain.LocalChainState state, String name, final List<OperatorNode<PhysicalExprOperator>> args) {
        throw new ProgramCompileException(location, "Source '%s' does not support SCAN and query has no matching index", name);
    }

    protected StreamValue deleteAll(Location location, final ContextPlanner planner, PlanChain.LocalChainState state, String name, final List<OperatorNode<PhysicalExprOperator>> args) {
        throw new ProgramCompileException(location, "Source '%s' does not support DELETE_ALL", name);
    }

    protected StreamValue updateAll(Location location, final ContextPlanner planner, PlanChain.LocalChainState state, String name, final List<OperatorNode<PhysicalExprOperator>> args, OperatorNode<PhysicalExprOperator> record) {
        throw new ProgramCompileException(location, "Source '%s' does not support UPDATE_ALL", name);
    }

    protected StreamValue insert(Location location, final ContextPlanner planner, PlanChain.LocalChainState state, String name, final List<OperatorNode<PhysicalExprOperator>> args, StreamValue records) {
        throw new ProgramCompileException(location, "Source '%s' does not support INSERT", name);
    }

    protected void indexQuery(List<StreamValue> out, Location location, ContextPlanner planner, List<IndexQuery> queries, List<OperatorNode<PhysicalExprOperator>> args) {
        // base class can automatically handle scattering to individual index lookups
        for (IndexQuery query : queries) {
            singleIndexQuery(out, location, planner, query, args);
        }
    }

    protected void singleIndexQuery(List<StreamValue> out, Location location, ContextPlanner planner, IndexQuery query, List<OperatorNode<PhysicalExprOperator>> args) {
        throw new ProgramCompileException("missing IndexSourceType implementation (must implement either either single or multi index query!)");
    }

    protected void deleteQuery(List<StreamValue> out, Location location, ContextPlanner planner, List<IndexQuery> queries, List<OperatorNode<PhysicalExprOperator>> args) {
        for (IndexQuery query : queries) {
            singleDeleteQuery(out, location, planner, query, args);
        }
    }

    protected void singleDeleteQuery(List<StreamValue> out, Location location, ContextPlanner planner, IndexQuery query, List<OperatorNode<PhysicalExprOperator>> args) {
        throw new ProgramCompileException("Source does not support DELETE");
    }

    protected void updateQuery(List<StreamValue> out, Location location, ContextPlanner planner, List<IndexQuery> queries, List<OperatorNode<PhysicalExprOperator>> args, OperatorNode<PhysicalExprOperator> record) {
        for (IndexQuery query : queries) {
            singleUpdateQuery(out, location, planner, query, args, record);
        }
    }

    protected void singleUpdateQuery(List<StreamValue> out, Location location, ContextPlanner planner, IndexQuery query, List<OperatorNode<PhysicalExprOperator>> args, OperatorNode<PhysicalExprOperator> record) {
        throw new ProgramCompileException("Source does not support DELETE");
    }

    public static class IndexQuery {
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

    @Override
    public StreamValue plan(final ContextPlanner planner, OperatorNode<SequenceOperator> query, final OperatorNode<SequenceOperator> source) {
        return new IndexPlanChain(planner).execute(query);
    }

    @Override
    public StreamValue join(ContextPlanner planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source) {
        return new IndexPlanChain(planner, leftSide, joinExpression).execute(query);
    }

    private class IndexPlanChain extends PlanChain {
        private final OperatorNode<PhysicalExprOperator> leftSide;
        private final OperatorNode<ExpressionOperator> joinExpression;

        public IndexPlanChain(ContextPlanner planner) {
            super(planner);
            this.leftSide = null;
            this.joinExpression = null;
        }

        public IndexPlanChain(ContextPlanner planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression) {
            super(planner);
            this.leftSide = leftSide;
            this.joinExpression = joinExpression;
        }


        @Override
        protected StreamValue executeSource(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> query) {
            switch (query.getOperator()) {
                case SCAN:
                    return executeSelect(context, state, query);
                case INSERT:
                    return executeInsert(context, state, query);
                case DELETE:
                case DELETE_ALL:
                    return executeDelete(context, state, query);
                case UPDATE_ALL:
                case UPDATE:
                    return executeUpdate(context, state, query);
                default:
                    throw new ProgramCompileException("Unsupported operation on source: %s", query);

            }
        }

        private StreamValue executeInsert(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> query) {
            Location location = query.getLocation();
            OperatorNode<SequenceOperator> source = query.getArgument(0);
            OperatorNode<SequenceOperator> records = query.getArgument(1);
            Preconditions.checkArgument(source.getOperator() == SequenceOperator.SCAN, "INSERT source argument must be SCAN, is %s", source);
            String name = Joiner.on(".").join(source.<Iterable<String>>getArgument(0));
            List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
            return insert(location, context, state, name, context.evaluateList(args), context.execute(records));
        }

        private StreamValue executeDelete(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> query) {
            Location location = query.getLocation();
            OperatorNode<SequenceOperator> source = query.getArgument(0);
            Preconditions.checkArgument(source.getOperator() == SequenceOperator.SCAN, "DELETE[_ALL] source argument must be SCAN, is %s", source);
            String name = Joiner.on(".").join(source.<Iterable<String>>getArgument(0));
            List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
            if (query.getOperator() == SequenceOperator.DELETE_ALL) {
                return deleteAll(location, context, state, name, context.evaluateList(args));
            }
            OperatorNode<ExpressionOperator> filter = query.getArgument(1);
            QueryStrategy qs = deletePlanner.planExact(filter);
            if (qs.scan) {
                // TODO: print index names into exception so people know what they CAN delete by?
                throw new ProgramCompileException(location, "DELETE must exactly match indexes (e.g. @Delete methods)");
            }
            List<OperatorNode<PhysicalExprOperator>> argExprs = context.evaluateList(args);
            // evaluate each argument once so we give precomputed values to each (of the possibly multiple) indexed invocations
            for (int i = 0; i < argExprs.size(); ++i) {
                argExprs.set(i, context.computeExpr(argExprs.get(i)));
            }
            List<StreamValue> outputs = Lists.newArrayList();
            for (IndexKey index : qs.indexes.keySet()) {
                Collection<IndexStrategy> strategyCollection = qs.indexes.get(index);
                List<IndexQuery> iqBatch = Lists.newArrayList();
                for (IndexStrategy strategy : strategyCollection) {
                    Preconditions.checkState(strategy.filter == null, "Internal error: index strategy planner for DELETE must use exact index matches");
                    IndexQuery iq = new IndexQuery();
                    prepareIndexKeyValues(context, strategy, iq);
                    iq.index = index;
                    iq.filter = strategy.filter;
                    iq.handledFilter = true;
                    iqBatch.add(iq);
                }
                deleteQuery(outputs, query.getLocation(), context, iqBatch, argExprs);
            }
            if (outputs.isEmpty()) {
                throw new ProgramCompileException("Unable to match execution strategy for query %s (? should not be reached given declared indexes)", query);
            }
            StreamValue result;
            if (outputs.size() == 1) {
                result = outputs.get(0);
            } else {
                result = context.merge(outputs);
            }
            state.setFilterHandled(true);
            return result;
        }

        private StreamValue executeUpdate(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> query) {
            Location location = query.getLocation();
            OperatorNode<SequenceOperator> source = query.getArgument(0);
            Preconditions.checkArgument(source.getOperator() == SequenceOperator.SCAN, "DELETE[_ALL] source argument must be SCAN, is %s", source);
            String name = Joiner.on(".").join(source.<Iterable<String>>getArgument(0));
            List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);

            OperatorNode<ExpressionOperator> record = query.getArgument(1);
            final OperatorNode<PhysicalExprOperator> recordValue = context.evaluate(record);

            if (query.getOperator() == SequenceOperator.UPDATE_ALL) {
                return updateAll(location, context, state, name, context.evaluateList(args), recordValue);
            }
            OperatorNode<ExpressionOperator> filter = query.getArgument(2);
            QueryStrategy qs = updatePlanner.planExact(filter);
            if (qs.scan) {
                // TODO: print index names into exception so people know what they CAN update by?
                throw new ProgramCompileException(location, "UPDATE must exactly match indexes (e.g. @Update methods)");
            }
            List<OperatorNode<PhysicalExprOperator>> argExprs = context.evaluateList(args);
            // evaluate each argument once so we give precomputed values to each (of the possibly multiple) indexed invocations
            for (int i = 0; i < argExprs.size(); ++i) {
                argExprs.set(i, context.computeExpr(argExprs.get(i)));
            }
            List<StreamValue> outputs = Lists.newArrayList();
            for (IndexKey index : qs.indexes.keySet()) {
                Collection<IndexStrategy> strategyCollection = qs.indexes.get(index);
                List<IndexQuery> iqBatch = Lists.newArrayList();
                for (IndexStrategy strategy : strategyCollection) {
                    Preconditions.checkState(strategy.filter == null, "Internal error: index strategy planner for UPDATE must use exact index matches");
                    IndexQuery iq = new IndexQuery();
                    prepareIndexKeyValues(context, strategy, iq);
                    iq.index = index;
                    iq.filter = strategy.filter;
                    iq.handledFilter = true;
                    iqBatch.add(iq);
                }
                updateQuery(outputs, query.getLocation(), context, iqBatch, argExprs, recordValue);
            }
            if (outputs.isEmpty()) {
                throw new ProgramCompileException("Unable to match execution strategy for query %s (? should not be reached given declared indexes)", query);
            }
            StreamValue result;
            if (outputs.size() == 1) {
                result = outputs.get(0);
            } else {
                result = context.merge(outputs);
            }
            state.setFilterHandled(true);
            return result;
        }

        private StreamValue executeSelect(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> query) {
            QueryStrategy qs = leftSide != null ?
                      indexPlanner.planJoin(leftSide, joinExpression, state.filter)
                    : indexPlanner.plan(state.filter);
            String name = Joiner.on(".").join(query.<Iterable<String>>getArgument(0));
            List<OperatorNode<ExpressionOperator>> args = query.getArgument(1);
            List<OperatorNode<PhysicalExprOperator>> argExprs = context.evaluateList(args);
            if (qs.scan) {
                return scan(query.getLocation(), context, state, name, argExprs);
            }
            // evaluate each argument once so we give precomputed values to each (of the possibly multiple) indexed invocations
            for (int i = 0; i < argExprs.size(); ++i) {
                argExprs.set(i, context.computeExpr(argExprs.get(i)));
            }

            List<StreamValue> outputs = Lists.newArrayList();
            boolean handledFilter = true;
            for (IndexKey index : qs.indexes.keySet()) {
                Collection<IndexStrategy> strategyCollection = qs.indexes.get(index);
                List<IndexQuery> iqBatch = Lists.newArrayList();
                for (IndexStrategy strategy : strategyCollection) {
                    IndexQuery iq = new IndexQuery();
                    prepareIndexKeyValues(context, strategy, iq);
                    iq.index = index;
                    iq.filter = strategy.filter;
                    if (strategy.filter != null) {
                        iq.filterPredicate = compileFilter(strategy.filter);
                        iq.handledFilter = false;
                    } else {
                        iq.handledFilter = true;
                    }
                    iqBatch.add(iq);
                }
                indexQuery(outputs, query.getLocation(), context, iqBatch, argExprs);
                for (IndexQuery iq : iqBatch) {
                    handledFilter = handledFilter && iq.handledFilter;
                }
            }
            if (outputs.isEmpty()) {
                throw new ProgramCompileException("Unable to match execution strategy for query %s (? should not be reached given declared indexes)", query);
            }
            StreamValue result;
            if (outputs.size() == 1) {
                result = outputs.get(0);
            } else {
                result = context.merge(outputs);
            }
            if (handledFilter) {
                state.setFilterHandled(true);
            }
            return result;
        }

        private void prepareIndexKeyValues(ContextPlanner context, IndexStrategy strategy, IndexQuery iq) {
            if(strategy.indexFilter != null) {
                for (Map.Entry<String, OperatorNode<ExpressionOperator>> e : strategy.indexFilter.entrySet()) {
                    String key = e.getKey();
                    OperatorNode<ExpressionOperator> zip = e.getValue();
                    OperatorNode<PhysicalExprOperator> keyExpr = context.evaluate((OperatorNode<ExpressionOperator>) zip.getArgument(1));
                    switch (zip.getOperator()) {
                        case EQ:
                            keyExpr = OperatorNode.create(keyExpr.getLocation(), PhysicalExprOperator.ARRAY, ImmutableList.of(keyExpr));
                            break;
                        case IN:
                            break;
                        default:
                            throw new ProgramCompileException("unknown ZipMatchOperator: %s", zip);
                    }
                    com.yahoo.yqlplus.api.index.IndexColumn column = strategy.descriptor.getColumn(key);
                    keyExpr = filterKeyArray(column, keyExpr);
                    iq.keyValues.put(key, keyExpr);
                }
            }
            if(strategy.joinColumns != null && !strategy.joinColumns.isEmpty()) {
                if(leftSide == null || joinExpression == null) {
                    throw new NullPointerException("joinColumns is non-empty yet there is no available leftSide");
                }
                // generate an expression to extract the relevant keys from the left side
                List<JoinExpression> join = JoinExpression.parse(joinExpression);
                List<String> fields = Lists.newArrayListWithExpectedSize(join.size());
                List<OperatorNode<PhysicalExprOperator>> expressions = Lists.newArrayListWithExpectedSize(join.size());
                ExprScope scope = new ExprScope();
                scope.addArgument("$row");
                final OperatorNode<PhysicalExprOperator> rowReference = OperatorNode.create(PhysicalExprOperator.LOCAL, "$row");
                DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(context, rowReference);
                List<OperatorNode<PhysicalExprOperator>> nullTests = Lists.newArrayListWithExpectedSize(join.size());
                for(JoinExpression expr : join) {
                    String rightField = expr.getRightField();
                    if(strategy.joinColumns.contains(rightField)) {
                        fields.add(rightField);
                        expressions.add(eval.apply(expr.left));
                        final OperatorNode<PhysicalExprOperator> fieldValue = OperatorNode.create(PhysicalExprOperator.PROPREF, rowReference, rightField);
                        com.yahoo.yqlplus.api.index.IndexColumn column = strategy.descriptor.getColumn(rightField);
                        if(column.isSkipNull() || column.isSkipEmpty()) {
                            nullTests.add(OperatorNode.create(PhysicalExprOperator.IS_NULL,
                                    fieldValue));
                        }
                        if(column.isSkipEmpty()) {
                            addEmptyTest(nullTests, column, fieldValue);
                        }
                    }
                }
                OperatorNode<FunctionOperator> keyFunction =
                        scope.createFunction(OperatorNode.create(PhysicalExprOperator.RECORD, fields, expressions));
                StreamValue leftKeys = StreamValue.iterate(context, leftSide);
                leftKeys.add(Location.NONE, StreamOperator.TRANSFORM, keyFunction);
                if(!nullTests.isEmpty()) {
                    OperatorNode<PhysicalExprOperator> anyNull = nullTests.size() == 1 ? nullTests.get(0) : OperatorNode.create(PhysicalExprOperator.OR, nullTests);
                    OperatorNode<FunctionOperator> filterFunction =
                            scope.createFunction(OperatorNode.create(PhysicalExprOperator.NOT, anyNull));
                    leftKeys.add(Location.NONE, StreamOperator.FILTER, filterFunction);
                }
                leftKeys.add(Location.NONE, StreamOperator.DISTINCT);
                iq.joinKeyStream = leftKeys;
                iq.joinKeys = fields;
            }
        }
    }

    private OperatorNode<PhysicalExprOperator> filterKeyArray(IndexColumn column, OperatorNode<PhysicalExprOperator> keyExpr) {
        final OperatorNode<PhysicalExprOperator> keyReference = OperatorNode.create(PhysicalExprOperator.LOCAL, "$key");
        List<OperatorNode<PhysicalExprOperator>> tests = Lists.newArrayList();
        if(column.isSkipNull() || column.isSkipEmpty()) {
            tests.add(OperatorNode.create(PhysicalExprOperator.IS_NULL, keyReference));
        }
        if(column.isSkipEmpty()) {
            addEmptyTest(tests, column, keyReference);
        }
        if(tests.isEmpty()) {
            return keyExpr;
        }
        ExprScope scope = new ExprScope();
        scope.addArgument("$key");
        OperatorNode<PhysicalExprOperator> test;
        if(tests.size() == 1) {
            test = tests.get(0);
        } else {
            test = OperatorNode.create(PhysicalExprOperator.OR, tests);
        }
        OperatorNode<FunctionOperator> filter = scope.createFunction(OperatorNode.create(PhysicalExprOperator.NOT, test));
        return OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE,
                keyExpr,
                OperatorNode.create(StreamOperator.FILTER,
                        OperatorNode.create(StreamOperator.SINK, OperatorNode.create(SinkOperator.ACCUMULATE)),
                        filter));
    }

    private void addEmptyTest(List<OperatorNode<PhysicalExprOperator>> emptyTests, IndexColumn column, OperatorNode<PhysicalExprOperator> fieldValue) {
        switch(column.getType().getCoreType()) {
            case INT32:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.INT32, 0)));
                break;
            case INT64:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.INT64, 0L)));
                break;
            case FLOAT32:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.FLOAT32, 0.0f)));
                break;
            case FLOAT64:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.FLOAT64, 0.0)));
                break;
            case STRING:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, NotNullableTypeWidget.create(BaseTypeAdapter.STRING), "")));
                break;
        }
    }

}
