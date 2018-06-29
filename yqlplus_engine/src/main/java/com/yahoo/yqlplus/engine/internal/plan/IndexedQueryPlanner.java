/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.compiler.runtime.Chooser;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.rules.IndexMatchOperatorTransform;
import com.yahoo.yqlplus.engine.rules.PushAndTransform;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extract logic for planning a query against an indexed source.
 */
public class IndexedQueryPlanner {
    private static Iterable<IndexDescriptor> convertSet(Set<IndexKey> indexes) {
        List<IndexDescriptor> desc = Lists.newArrayListWithCapacity(indexes.size());
        for(IndexKey key : indexes) {
            IndexDescriptor.Builder b = IndexDescriptor.builder();
            for(String name : key.columnOrder) {
                b.addColumn(name, YQLCoreType.STRING, false, true);
            }
            desc.add(b.build());
        }
        return desc;
    }


    final Set<String> indexColumns;
    final Map<IndexKey, IndexDescriptor> indexes;

    public IndexedQueryPlanner(Set<IndexKey> indexes) {
        this(convertSet(indexes));
    }

    public IndexedQueryPlanner(Iterable<IndexDescriptor> indexes) {
        ImmutableMap.Builder<IndexKey, IndexDescriptor> idx = ImmutableMap.builder();
        ImmutableSet.Builder<String> cols = ImmutableSet.builder();
        for (IndexDescriptor index : indexes) {
            idx.put(IndexKey.of(index.getColumnNames()), index);
            cols.addAll(index.getColumnNames());
        }
        this.indexes = idx.build();
        this.indexColumns = cols.build();
    }


    /**
     * Make a query execution plan where all filter elements MUST exactly match an index.
     *
     * @param filter
     * @return
     */
    public QueryStrategy planExact(OperatorNode<ExpressionOperator> filter) {
        return planInternal(ImmutableSet.of(), filter, true);
    }

    public QueryStrategy plan(OperatorNode<ExpressionOperator> filter) {
        return planInternal(ImmutableSet.of(), filter, false);
    }

    public QueryStrategy planJoin(OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<ExpressionOperator> filter) {
        List<JoinExpression> clauses = JoinExpression.parse(joinExpression);
        Set<String> joinColumns = Sets.newHashSet();
        for(JoinExpression join : clauses) {
            if(join.right.getOperator() == ExpressionOperator.READ_FIELD) {
                joinColumns.add(join.right.getArgument(1));
            }
        }
        return planInternal(joinColumns, filter, false);
    }

    private QueryStrategy planInternal(Set<String> availableJoinColumns, OperatorNode<ExpressionOperator> filter, boolean exact) {
        QueryStrategy iq = new QueryStrategy();
        if (filter == null && availableJoinColumns.isEmpty()) {
            iq.scan = true;
        } else if(filter == null) {
            IndexKey index = matchIndex(availableJoinColumns, exact);
            if (index == null) {
                iq.scan = true;
            } else {
                List<String> indexColumns = index.columnOrder;
                // add the entry to our query strategy
                IndexStrategy indexStrategy = new IndexStrategy(index, indexes.get(index));
                indexStrategy.filter = null;
                indexStrategy.indexFilter = null;
                indexStrategy.joinColumns = indexColumns;
                iq.add(indexStrategy);
            }
        } else {
            // now we need to interpret the filter
            // first, expand variable references
            // this is done in the engine now
            // filter = new ExpandVariablesTransform(runtime).visitExpr(filter);
            // push ANDs into leaves of ORs
            filter = new PushAndTransform().visitExpr(filter);
            // at this point all ORs should have been pulled to the top level, now let's build up batches of queries to indexes
            filter = new IndexMatchOperatorTransform().visitExpr(filter);

            // for now we'll plan to re-evaluate the filter expression completely per row (the unmodified one); that allows this code
            // to not worry about that and instead to focus on candidates for index query inputs


            // each OR clause will be a new stream
            //   for each stream
            //     extract the relevant index clauses
            //     if none match then SCAN
            // if ANY OR clause SCANs then SCAN
            //    a clause may be FULLY handled if it *exactly* matches an index
            //    if all clauses are FULLY handled, then we need not filter
            // we can simplify the filter if SOME clauses match and no tricky ORs eliminate things

            // transform a filter clause into:
            //     index (input exprs)
            //     (index (input exprs), filter (if index fully handles filter, then there is none)
            //     SCAN, filter
            // if any matches SCAN, just SCAN + initial filter
            // otherwise: dispatch each index operation in parallel

            // we SHOULD require or at least support that rows contain a row-id so they can union things even when multiple indexes match
            // since we'll be dispatching multiple requests to the same source
            // skip that for now

            if (filter.getOperator() == ExpressionOperator.OR) {
                List<OperatorNode<ExpressionOperator>> clauses = filter.getArgument(0);
                for (OperatorNode<ExpressionOperator> clause : clauses) {
                    prepareQuery(availableJoinColumns, iq, clause, exact);
                }
            } else {
                prepareQuery(availableJoinColumns, iq, filter, exact);
            }
        }
        return iq;
    }

    private void prepareQuery(Set<String> availableJoinColumns, QueryStrategy iq, OperatorNode<ExpressionOperator> filter, boolean exact) {
        // if we've issued a SCAN, skip this logic and just scan
        if (iq.scan) {
            return;
        }
        // we can handle the following scenarios:
        //   EQ READ_FIELD *
        //   EQ * READ_FIELD
        //   IN READ_FIELD *
        //   ZIP_MATCH
        //      (appropriate combinations of AND, EQ, and IN are transformed in the ZipMatchOperatorTransform)
        // we'll transform the input filter into
        //   candidate column ZipMatchOperators
        //   everything-else
        // logically these are ANDed together, but only the candidate column filters can be used for indexes
        // we'll match the most precise combination of columns for available indexes, and move any remaining columns to everything-else

        Map<String, OperatorNode<ExpressionOperator>> columns = Maps.newHashMap();
        List<OperatorNode<ExpressionOperator>> others = Lists.newArrayList();
        if (filter.getOperator() == ExpressionOperator.AND) {
            List<OperatorNode<ExpressionOperator>> clauses = filter.getArgument(0);
            for (OperatorNode<ExpressionOperator> clause : clauses) {
                processFilterClause(columns, others, clause);
            }
        } else {
            processFilterClause(columns, others, filter);
        }

        IndexKey index = matchIndex(Sets.union(availableJoinColumns, columns.keySet()), exact);
        if (index == null) {
            iq.scan = true;
            return;
        }

        // splendid, we matched an index -- rearrange the columns / others according to the matched index
        List<String> indexColumns = Lists.newArrayList(index.columnOrder);
        List<OperatorNode<ExpressionOperator>> unmatched = Lists.newArrayList();
        Iterator<Map.Entry<String, OperatorNode<ExpressionOperator>>> cols = columns.entrySet().iterator();
        while (cols.hasNext()) {
            Map.Entry<String, OperatorNode<ExpressionOperator>> clause = cols.next();
            if (!indexColumns.contains(clause.getKey())) {
                unmatched.add(clause.getValue());
                cols.remove();
            } else {
                indexColumns.remove(clause.getKey());
            }
        }
        if (!unmatched.isEmpty()) {
            others.add(OperatorNode.create(ExpressionOperator.AND, unmatched));
        }

        // add the entry to our query strategy
        IndexStrategy indexStrategy = new IndexStrategy(index, indexes.get(index));
        if (others.isEmpty()) {
            indexStrategy.filter = null;
        } else if (others.size() == 1) {
            indexStrategy.filter = others.get(0);
        } else {
            indexStrategy.filter = OperatorNode.create(ExpressionOperator.AND, others);
        }
        indexStrategy.indexFilter = columns;
        if(!indexColumns.isEmpty()) {
            indexStrategy.joinColumns = indexColumns;
        }
        iq.add(indexStrategy);
    }

    private IndexKey matchIndex(Set<String> columns, boolean exact) {
        if (exact) {
            IndexKey terms = IndexKey.of(columns);
            if (indexes.containsKey(terms)) {
                return terms;
            }
        } else {
            Chooser<String> chooser = Chooser.create();
            List<String> terms = Lists.newArrayList(columns);
            for (int choose = terms.size(); choose > 0; --choose) {
                for (List<String> candidate : chooser.chooseN(choose, terms)) {
                    IndexKey key = IndexKey.of(candidate);
                    if (indexes.containsKey(key)) {
                        return key;
                    }
                }
            }
        }
        return null;
    }

    private void processFilterClause(Map<String, OperatorNode<ExpressionOperator>> columns,
                                     List<OperatorNode<ExpressionOperator>> others,
                                     OperatorNode<ExpressionOperator> filter) {
        switch (filter.getOperator()) {
            case EQ: {
                OperatorNode<ExpressionOperator> left = filter.getArgument(0);
                OperatorNode<ExpressionOperator> right = filter.getArgument(1);
                String leftName = extractFieldMatch(left);
                String rightName = extractFieldMatch(right);
                if (leftName != null && rightName == null && indexColumns.contains(leftName)) {
                    columns.put(leftName, OperatorNode.create(filter.getLocation(), ExpressionOperator.EQ, left, right));
                } else if (rightName != null && leftName != null && indexColumns.contains(rightName)) {
                    columns.put(rightName, OperatorNode.create(filter.getLocation(), ExpressionOperator.EQ, right, left));
                } else {
                    others.add(filter);
                }
                break;
            }
            case IN: {
                OperatorNode<ExpressionOperator> left = filter.getArgument(0);
                OperatorNode<ExpressionOperator> right = filter.getArgument(1);
                String leftField = extractFieldMatch(left);
                if (left.getOperator() == ExpressionOperator.READ_FIELD && indexColumns.contains(leftField)) {
                    columns.put(leftField, OperatorNode.create(filter.getLocation(), filter.getAnnotations(), ExpressionOperator.IN, left, right));
                } else {
                    others.add(filter);
                }
                break;
            }
            default:
                others.add(filter);
                break;
        }
    }

    private String extractFieldMatch(OperatorNode<ExpressionOperator> expr) {
        if (expr.getOperator() == ExpressionOperator.READ_FIELD) {
            String alias = expr.getArgument(0);
            String fieldName = expr.getArgument(1);
            // TODO: we should pay attention to the alias of READ_FIELD to verify that we're picking up a field for "this" source
            // (for now we ignore this because the engine won't push filters down unless they apply only to a given source when multiple sources
            // are present)
            return fieldName.toLowerCase();
        }
        return null;
    }

}
