/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.grammar;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.ProjectOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test parsing of syntax for FROM SOURCES (* | source_list)
 */
@Test
public class ExpressionParsingTest {
    @Test
    public void testParseLongs() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "select 8589934592L OUTPUT AS a;");
        OperatorNode<SequenceOperator> all = OperatorNode.create(SequenceOperator.PROJECT,
                OperatorNode.create(SequenceOperator.EMPTY),
                ImmutableList.of(
                        OperatorNode.create(ProjectOperator.FIELD, OperatorNode.create(ExpressionOperator.LITERAL, 8589934592L), "expr")
                )
        );
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        all, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }

    @Test
    public void testParseAdd() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "select 10L + 20L OUTPUT AS a;");
        OperatorNode<SequenceOperator> all = OperatorNode.create(SequenceOperator.PROJECT,
                OperatorNode.create(SequenceOperator.EMPTY),
                ImmutableList.of(
                        OperatorNode.create(ProjectOperator.FIELD,
                                OperatorNode.create(ExpressionOperator.ADD,
                                        OperatorNode.create(ExpressionOperator.LITERAL, 10L),
                                        OperatorNode.create(ExpressionOperator.LITERAL, 20L))

                                , "expr")
                )
        );
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        all, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }

    // this is not yet valid
    @Test(expectedExceptions = ProgramCompileException.class)
    public void testParseMap() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "select { 10L : 'foo' } OUTPUT AS a;");
        OperatorNode<SequenceOperator> all = OperatorNode.create(SequenceOperator.PROJECT,
                OperatorNode.create(SequenceOperator.EMPTY),
                ImmutableList.of(
                        OperatorNode.create(ProjectOperator.FIELD, OperatorNode.create(ExpressionOperator.LITERAL, 858993459L), "expr")
                )
        );
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        all, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }

    @Test
    public void testParseMap2() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        // (PROGRAM [(EXECUTE L0:1 (PROJECT (EMPTY), [(FIELD (MAP L9:1 [foo], [(LITERAL L17:1 10)]), expr)]), a), (OUTPUT L0:1 a)])
        OperatorNode<StatementOperator> program = parser.parse("sources.yql", "select { 'foo' : 10L } OUTPUT AS a;");
        OperatorNode<SequenceOperator> all = OperatorNode.create(SequenceOperator.PROJECT,
                OperatorNode.create(SequenceOperator.EMPTY),
                ImmutableList.of(
                        OperatorNode.create(ProjectOperator.FIELD,
                                OperatorNode.create(ExpressionOperator.MAP,
                                        ImmutableList.of("foo"),
                                        ImmutableList.of(OperatorNode.create(ExpressionOperator.LITERAL, 10L))
                                )
                                , "expr")
                )
        );
        Assert.assertEquals(program, OperatorNode.create(StatementOperator.PROGRAM,
                ImmutableList.of(OperatorNode.create(StatementOperator.EXECUTE,
                        all, "a"),
                        OperatorNode.create(StatementOperator.OUTPUT, "a"))));
    }
}
