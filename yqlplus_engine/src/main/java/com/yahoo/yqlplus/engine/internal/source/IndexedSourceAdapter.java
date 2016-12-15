/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.source;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.IndexKey;
import com.yahoo.yqlplus.engine.internal.plan.IndexedQueryPlanner;
import com.yahoo.yqlplus.engine.internal.plan.IndexedSourceType;
import com.yahoo.yqlplus.engine.internal.plan.PlanChain;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IndexedSourceAdapter extends IndexedSourceType {
    private final TypeWidget adapterClass;
    private final String name;
    private final QueryMethod selectAll;
    private final Map<IndexKey, QueryMethod> selectMap;

    private final Map<IndexKey, UpdateMethod> updateMap;
    private final UpdateMethod updateAll;

    private final Map<IndexKey, QueryMethod> deleteMap;
    private final QueryMethod deleteAll;

    private final InsertMethod insert;

    public IndexedSourceAdapter(TypeWidget adapterClass,
                                String name, Map<IndexDescriptor, QueryMethod> methodMap, QueryMethod selectAll,
                                Map<IndexDescriptor, QueryMethod> deleteMap, QueryMethod deleteAll,
                                Map<IndexDescriptor, UpdateMethod> updateMap, UpdateMethod updateAll,
                                InsertMethod insert) {
        super(createPlanner(methodMap), createPlanner(deleteMap), createPlanner(updateMap));
        this.adapterClass = adapterClass;
        this.name = name;
        this.selectAll = selectAll;
        this.selectMap = createIndexMap(methodMap);
        this.deleteMap = createIndexMap(deleteMap);
        this.deleteAll = deleteAll;
        this.updateMap = createIndexMap(updateMap);
        this.updateAll = updateAll;
        this.insert = insert;
    }

    private static IndexedQueryPlanner createPlanner(Map<IndexDescriptor, ?> methodMap) {
        return new IndexedQueryPlanner(methodMap.keySet());
    }

    private static <T> Map<IndexKey, T> createIndexMap(Map<IndexDescriptor, T> methodMap) {
        Map<IndexKey, T> indexes = Maps.newLinkedHashMap();
        for (Map.Entry<IndexDescriptor, T> e : methodMap.entrySet()) {
            indexes.put(IndexKey.of(e.getKey().getColumnNames()), e.getValue());
        }
        return indexes;
    }

    @Override
    protected StreamValue scan(Location location, ContextPlanner planner, PlanChain.LocalChainState state, String name, List<OperatorNode<PhysicalExprOperator>> args) {
        if (selectAll == null) {
            throw new ProgramCompileException(location, "Source '%s' does not enable SCAN (all @Query methods have @Key or @CompoundKey arguments)", this.name);
        }
        return selectAll.scan(location, createSource(location, planner, args), planner, state);
    }

    @Override
    protected StreamValue insert(Location location, ContextPlanner planner, PlanChain.LocalChainState state, String name, List<OperatorNode<PhysicalExprOperator>> args, StreamValue records) {
        if (insert == null) {
            throw new ProgramCompileException(location, "Source '%s' does not enable INSERT (has no @Insert method)", this.name);
        }
        return insert.insert(location, createSource(location, planner, args), planner, state, records);
    }

    private OperatorNode<PhysicalExprOperator> createSource(Location location, ContextPlanner planner, List<OperatorNode<PhysicalExprOperator>> arguments) {
        return planner.computeExpr(
                OperatorNode.create(location, PhysicalExprOperator.INJECT_MEMBERS,
                        OperatorNode.create(location, PhysicalExprOperator.NEW,
                                adapterClass,
                                arguments)));
    }

    @Override
    protected void indexQuery(List<StreamValue> out, Location location, ContextPlanner planner, List<IndexQuery> queries, List<OperatorNode<PhysicalExprOperator>> args) {
        // split IndexQuery by index and then invoke each index exactly once
        OperatorNode<PhysicalExprOperator> sourceAdapter = createSource(location, planner, args);
        Multimap<IndexKey, IndexQuery> split = ArrayListMultimap.create();
        for (IndexQuery query : queries) {
            split.put(query.index, query);
        }
        for (IndexKey idx : split.keySet()) {
            Collection<IndexQuery> todo = split.get(idx);
            selectMap.get(idx).index(out, location, sourceAdapter, planner, Lists.newArrayList(todo));
        }
    }

    @Override
    protected StreamValue deleteAll(Location location, ContextPlanner planner, PlanChain.LocalChainState state, String name, List<OperatorNode<PhysicalExprOperator>> args) {
        if (deleteAll == null) {
            throw new ProgramCompileException(location, "Source '%s' does not enable DELETE_ALL (all @Delete methods have @Key or @CompoundKey arguments)", this.name);
        }
        return deleteAll.scan(location, createSource(location, planner, args), planner, state);
    }

    @Override
    protected void deleteQuery(List<StreamValue> out, Location location, ContextPlanner planner, List<IndexQuery> queries, List<OperatorNode<PhysicalExprOperator>> args) {
        // split IndexQuery by index and then invoke each index exactly once
        OperatorNode<PhysicalExprOperator> sourceAdapter = createSource(location, planner, args);
        Multimap<IndexKey, IndexQuery> split = ArrayListMultimap.create();
        for (IndexQuery query : queries) {
            split.put(query.index, query);
        }
        for (IndexKey idx : split.keySet()) {
            Collection<IndexQuery> todo = split.get(idx);
            deleteMap.get(idx).index(out, location, sourceAdapter, planner, Lists.newArrayList(todo));
        }
    }

    @Override
    protected StreamValue updateAll(Location location, ContextPlanner planner, PlanChain.LocalChainState state, String name, List<OperatorNode<PhysicalExprOperator>> args, OperatorNode<PhysicalExprOperator> record) {
        if (updateAll == null) {
            throw new ProgramCompileException(location, "Source '%s' does not enable UPDATE_ALL (all @Update methods have @Key or @CompoundKey arguments)", this.name);
        }
        return updateAll.all(location, createSource(location, planner, args), planner, state, record);
    }

    @Override
    protected void updateQuery(List<StreamValue> out, Location location, ContextPlanner planner, List<IndexQuery> queries, List<OperatorNode<PhysicalExprOperator>> args, OperatorNode<PhysicalExprOperator> record) {
        OperatorNode<PhysicalExprOperator> sourceAdapter = createSource(location, planner, args);
        Multimap<IndexKey, IndexQuery> split = ArrayListMultimap.create();
        for (IndexQuery query : queries) {
            split.put(query.index, query);
        }
        for (IndexKey idx : split.keySet()) {
            Collection<IndexQuery> todo = split.get(idx);
            updateMap.get(idx).index(out, location, sourceAdapter, planner, Lists.newArrayList(todo), record);
        }
    }
}
