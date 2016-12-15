/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test the filter join rule
 */
@Test
public class ExpandViewsTest {
    static class Registry implements ViewRegistry {
        private Map<String, OperatorNode<SequenceOperator>> views = Maps.newHashMap();

        public void defineView(String name, OperatorNode<SequenceOperator> view) {
            views.put(name, view);
        }

        @Override
        public OperatorNode<SequenceOperator> getView(List<String> name) {
            Preconditions.checkArgument(name.size() == 1);
            return views.get(name.get(0));
        }
    }

    @Test
    public void testExpandView() throws IOException {
        OperatorNode<SequenceOperator> query = OperatorNode.create(SequenceOperator.FILTER,
                OperatorNode.create(SequenceOperator.JOIN,
                        OperatorNode.create(SequenceOperator.SCAN, Lists.newArrayList("left"), ImmutableList.of()).putAnnotation("alias", "left"),
                        OperatorNode.create(SequenceOperator.SCAN, Lists.newArrayList("right"), ImmutableList.of()).putAnnotation("alias", "right"),
                        OperatorNode.create(ExpressionOperator.EQ,
                                OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "left"), "id"),
                                OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "right"), "id"))),
                OperatorNode.create(ExpressionOperator.EQ,
                        OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "left"), "id"),
                        OperatorNode.create(ExpressionOperator.LITERAL, "1")));
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.FILTER);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), SequenceOperator.SCAN);

        // now expand the view
        Registry scope = new Registry();

        scope.defineView("left", OperatorNode.create(SequenceOperator.FILTER,
                OperatorNode.create(SequenceOperator.SCAN, Lists.newArrayList("fancy"), ImmutableList.of()).putAnnotation("alias", "fancy"),
                OperatorNode.create(ExpressionOperator.EQ,
                        OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "fancy"), "id"),
                        OperatorNode.create(ExpressionOperator.LITERAL, "1"))));

        transformed = new ExpandViews(scope).visitSequenceOperator(transformed);
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.FILTER);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), SequenceOperator.SCAN);

        OperatorNode<SequenceOperator> left = (OperatorNode<SequenceOperator>) transformed.getArgument(0);
        // FILTER FILTER SCAN (already checked that left is FILTER, so the next should be too
        Assert.assertEquals(((OperatorNode) left.getArgument(0)).getOperator(), SequenceOperator.FILTER);
        // now look for the scan
        OperatorNode<SequenceOperator> scan = (OperatorNode) ((OperatorNode) left.getArgument(0)).getArgument(0);
        Assert.assertEquals(scan.getOperator(), SequenceOperator.SCAN);
        Assert.assertEquals(scan.getArgument(0), ImmutableList.of("fancy"));

    }
}
