/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test the filter join rule
 */
@Test
public class JoinFilterPushDownTest {
    @Test
    public void testLeftPush() throws IOException {
        OperatorNode<SequenceOperator> query = OperatorNode.create(SequenceOperator.FILTER,
                OperatorNode.create(SequenceOperator.JOIN,
                        OperatorNode.create(SequenceOperator.SCAN, ImmutableList.of("left"), Lists.newArrayList()).putAnnotation("alias", "left"),
                        OperatorNode.create(SequenceOperator.SCAN, ImmutableList.of("right"), Lists.newArrayList()).putAnnotation("alias", "right"),
                        OperatorNode.create(ExpressionOperator.EQ,
                                OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "left"), "id"),
                                OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "right"), "id"))
                ),
                OperatorNode.create(ExpressionOperator.EQ,
                        OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "left"), "id"),
                        OperatorNode.create(ExpressionOperator.LITERAL, "1"))
        );
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.FILTER);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), SequenceOperator.SCAN);
        // TODO: validate the rest of the tree
    }

    @Test
    public void testRightPush() throws IOException {
        OperatorNode<SequenceOperator> query = OperatorNode.create(SequenceOperator.FILTER,
                OperatorNode.create(SequenceOperator.JOIN,
                        OperatorNode.create(SequenceOperator.SCAN, ImmutableList.of("left"), Lists.newArrayList()).putAnnotation("alias", "left"),
                        OperatorNode.create(SequenceOperator.SCAN, ImmutableList.of("right"), Lists.newArrayList()).putAnnotation("alias", "right"),
                        OperatorNode.create(ExpressionOperator.EQ,
                                OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "left"), "id"),
                                OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "right"), "id"))
                ),
                OperatorNode.create(ExpressionOperator.EQ,
                        OperatorNode.create(ExpressionOperator.PROPREF, OperatorNode.create(ExpressionOperator.READ_RECORD, "right"), "id"),
                        OperatorNode.create(ExpressionOperator.LITERAL, "1"))
        );
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.SCAN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), SequenceOperator.FILTER);
        // TODO: validate the rest of the tree
    }

    @Test
    public void testNoPush() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<SequenceOperator> query = parser.parseQuery("SELECT * FROM left JOIN right ON left.id = right.id WHERE left.id = 1 OR right.id = 1");
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.FILTER);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.JOIN);
        // TODO: validate the rest of the tree
    }

    @Test
    public void testClausePush() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<SequenceOperator> query = parser.parseQuery("SELECT * FROM left JOIN right ON left.id = right.id WHERE left.category = 1 AND right.woeid = 2");
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        //System.err.println(transformed.toString());
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.FILTER);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), SequenceOperator.FILTER);
        // TODO: validate the rest of the tree
    }
    
    @Test
    public void testClausePushWithExpression() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<SequenceOperator> query = parser.parseQuery("SELECT * FROM left JOIN right ON left.id = right.id WHERE false AND left.category = 1 AND right.woeid = 2 AND true");
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        //System.err.println(transformed.toString());
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.FILTER);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), ExpressionOperator.AND);
        // TODO: validate the rest of the tree
    }

    @Test
    public void testMultiClausePush() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<SequenceOperator> query = parser.parseQuery(
                "SELECT * " +
                        "FROM left " +
                        "JOIN right ON left.id = right.id " +
                        "JOIN middle ON right.id = middle.id " +
                        "WHERE left.category = 1 AND right.woeid = 2 AND middle.id = 1"
        );
        OperatorNode<SequenceOperator> transformed = new JoinFilterPushDown().visitSequenceOperator(query);
        Assert.assertEquals(transformed.getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(0)).getOperator(), SequenceOperator.JOIN);
        Assert.assertEquals(((OperatorNode) transformed.getArgument(1)).getOperator(), SequenceOperator.FILTER);
        // TODO: validate the rest of the tree
    }
}
