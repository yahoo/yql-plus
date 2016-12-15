/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class IndexedQueryPlannerTest {
    @Test
    public void testIndexId() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1'"));
        Assert.assertFalse(strategy.scan);
        Assert.assertEquals(strategy.indexes.size(), 1);
        Collection<IndexStrategy> q = strategy.indexes.get(ID_INDEX);
        Assert.assertEquals(q.size(), 1);
        Iterator<IndexStrategy> strategyIterator = q.iterator();
        IndexStrategy strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L0:1 row, id), (LITERAL L5:1 1))");
    }

    @Test
    public void testIndexId2() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1' OR id = '2'"));
        Assert.assertFalse(strategy.scan);
        Assert.assertEquals(strategy.indexes.size(), 2);
        Collection<IndexStrategy> q = strategy.indexes.get(ID_INDEX);
        Assert.assertEquals(q.size(), 2);
        Iterator<IndexStrategy> strategyIterator = q.iterator();
        IndexStrategy strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L0:1 row, id), (LITERAL L5:1 1))");
        strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L12:1 row, id), (LITERAL L17:1 2))");
    }

    @Test
    public void testIndexIdScan() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1' OR pants = '2'"));
        Assert.assertTrue(strategy.scan);
    }

    @Test
    public void testIndexIdExtra() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1' AND pants = '2'"));
        Assert.assertFalse(strategy.scan);
        Assert.assertEquals(strategy.indexes.size(), 1);
        Collection<IndexStrategy> q = strategy.indexes.get(ID_INDEX);
        Assert.assertEquals(q.size(), 1);
        Iterator<IndexStrategy> strategyIterator = q.iterator();
        IndexStrategy strategyKey = strategyIterator.next();
        Assert.assertEquals(strategyKey.filter.toString(), "(EQ (READ_FIELD L13:1 row, pants), (LITERAL L21:1 2))");
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L0:1 row, id), (LITERAL L5:1 1))");
    }

    @Test
    public void testIndexIdPants() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        IndexKey PANTS_INDEX = IndexKey.of("id", "pants");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX,
                PANTS_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1' AND pants = '2'"));
        Assert.assertFalse(strategy.scan);
        Assert.assertEquals(strategy.indexes.size(), 1);
        Collection<IndexStrategy> q = strategy.indexes.get(PANTS_INDEX);
        Assert.assertEquals(q.size(), 1);
        Iterator<IndexStrategy> strategyIterator = q.iterator();
        IndexStrategy strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L0:1 row, id), (LITERAL L5:1 1))");
        Assert.assertEquals(strategyKey.indexFilter.get("pants").toString(), "(EQ (READ_FIELD L13:1 row, pants), (LITERAL L21:1 2))");
    }

    @Test
    public void testIndexIdPants2() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        IndexKey PANTS_INDEX = IndexKey.of("id", "pants");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX,
                PANTS_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1' AND pants IN ('2', '3', '4')"));
        Assert.assertFalse(strategy.scan);
        Assert.assertEquals(strategy.indexes.size(), 1);
        Collection<IndexStrategy> q = strategy.indexes.get(PANTS_INDEX);
        Assert.assertEquals(q.size(), 1);
        Iterator<IndexStrategy> strategyIterator = q.iterator();
        IndexStrategy strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L0:1 row, id), (LITERAL L5:1 1))");
        Assert.assertEquals(strategyKey.indexFilter.get("pants").toString(), "(IN (READ_FIELD L13:1 row, pants), (ARRAY L23:1 [(LITERAL L23:1 2), (LITERAL L28:1 3), (LITERAL L33:1 4)]))");
    }

    @Test
    public void testIndexIdPants3() throws Exception {
        IndexKey ID_INDEX = IndexKey.of("id");
        IndexKey PANTS_INDEX = IndexKey.of("id", "pants");
        Set<IndexKey> indexKeySet = Sets.newHashSet(
                ID_INDEX,
                PANTS_INDEX
        );
        IndexedQueryPlanner planner = new IndexedQueryPlanner(indexKeySet);
        QueryStrategy strategy = planner.plan(parseFilter("id = '1' AND pants IN ('2', '3', '4') OR id = '3'"));
        Assert.assertFalse(strategy.scan);
        Assert.assertEquals(strategy.indexes.size(), 2);
        Collection<IndexStrategy> q = strategy.indexes.get(PANTS_INDEX);
        Assert.assertEquals(q.size(), 1);
        Iterator<IndexStrategy> strategyIterator = q.iterator();
        IndexStrategy strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L0:1 row, id), (LITERAL L5:1 1))");
        Assert.assertEquals(strategyKey.indexFilter.get("pants").toString(), "(IN (READ_FIELD L13:1 row, pants), (ARRAY L23:1 [(LITERAL L23:1 2), (LITERAL L28:1 3), (LITERAL L33:1 4)]))");

        q = strategy.indexes.get(ID_INDEX);
        Assert.assertEquals(q.size(), 1);
        strategyIterator = q.iterator();
        strategyKey = strategyIterator.next();
        Assert.assertNull(strategyKey.filter);
        Assert.assertEquals(strategyKey.indexFilter.get("id").toString(), "(EQ (READ_FIELD L41:1 row, id), (LITERAL L46:1 3))");
    }

    private OperatorNode<ExpressionOperator> parseFilter(String filter) throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        return parser.parseExpression(filter, ImmutableSet.of("row"));
    }
}
