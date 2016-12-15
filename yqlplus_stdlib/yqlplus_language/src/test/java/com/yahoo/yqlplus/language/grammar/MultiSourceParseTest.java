/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.grammar;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test parsing of syntax for FROM SOURCES (* | source_list)
 */
@Test
public class MultiSourceParseTest {
    @Test
    public void testFromStar() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "SELECT * FROM SOURCES * OUTPUT AS a;");
        OperatorNode<SequenceOperator> all = OperatorNode.create(SequenceOperator.ALL);
        all.putAnnotation("alias", "row");
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        all, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }

    @Test
    public void testFromName() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "SELECT * FROM SOURCES joe OUTPUT AS a;");
        OperatorNode<SequenceOperator> multi = OperatorNode.create(SequenceOperator.MULTISOURCE,
                ImmutableList.of(ImmutableList.of("joe")));
        multi.putAnnotation("alias", "row");
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        multi, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }

    @Test
    public void testFromNames() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "SELECT * FROM SOURCES joe, smith, weather.id OUTPUT AS a;");
        OperatorNode<SequenceOperator> multi = OperatorNode.create(SequenceOperator.MULTISOURCE,
                ImmutableList.of(ImmutableList.of("joe"),
                        ImmutableList.<String>of("smith"),
                        ImmutableList.<String>of("weather", "id")));
        multi.putAnnotation("alias", "row");
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        multi, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }

    @Test
    public void testWithFilter() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "select ignoredfield from sources sourceA, sourceB where price <= 500;");
    }

    @Test
    public void testWithFilterAll() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "select ignoredfield from sources * where price <= 500;");
    }

}
