/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.grammar;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

//import org.antlr.v4.runtime.tree.CommonTree;

@Test
public class TestParsing {
    public ParseTree parseProgram(String program) throws RecognitionException, IOException {
        yqlplusParser parser = prepareParser(program);
        return parser.program();
    }

    public ParseTree parseExprAST(String expr) throws RecognitionException, IOException {
        yqlplusParser parser = prepareParser(expr);
        return parser.expression(true);
    }


    private yqlplusParser prepareParser(String input) throws IOException {
        return new ProgramParser().prepareParser("<string>", input);
    }

    @Test
    public void testScalarLiterals() throws RecognitionException, IOException {
        ParseTree tree = parseExprAST("1");
        OperatorNode<ExpressionOperator> operator = new ProgramParser().convertExpr(tree, null);
        assertEquals("(LITERAL L0:1 1)", operator.toString());

        tree = parseExprAST("1.0");
        operator = new ProgramParser().convertExpr(tree, null);
        assertEquals("(LITERAL L0:1 1.0)", operator.toString());

        tree = parseExprAST("'pants'");
        operator = new ProgramParser().convertExpr(tree, null);
        assertEquals("(LITERAL L0:1 pants)", operator.toString());

        tree = parseExprAST("'pants\\nbark'");
        operator = new ProgramParser().convertExpr(tree, null);
        assertEquals("(LITERAL L0:1 pants\nbark)", operator.toString());

        tree = parseExprAST("true");
        operator = new ProgramParser().convertExpr(tree, null);
        assertEquals("(LITERAL L0:1 true)", operator.toString());

        tree = parseExprAST("false");
        operator = new ProgramParser().convertExpr(tree, null);
        assertEquals("(LITERAL L0:1 false)", operator.toString());
    }

    enum Unit {
        PROGRAM,
        EXPRESSION
    }

    static class TestParseEntry {
        Unit unit;
        String input;
        String outputTree;

        TestParseEntry(Unit unit, String input, String outputTree) {
            this.unit = unit;
            this.input = input;
            this.outputTree = outputTree;
        }
    }

    List<TestParseEntry> parseTestTree(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        List<TestParseEntry> entries = Lists.newArrayList();
        String line = reader.readLine();
        while (line != null) {
            line = line.trim();
            if (!(line.startsWith("//") || line.isEmpty())) {
                Unit unit = Unit.PROGRAM;
                String input = line;
                if (input.startsWith("expression") || input.startsWith("program")) {
                    int idx = input.indexOf(' ');
                    unit = Unit.valueOf(input.substring(0, idx).toUpperCase());
                    input = input.substring(idx + 1);
                }
                String expected = reader.readLine();
                entries.add(new TestParseEntry(unit, input, expected));
            }
            line = reader.readLine();
        }
        return entries;
    }

    File recomputeParseTree(File target) throws IOException, RecognitionException {
        BufferedReader reader = new BufferedReader(new FileReader(target));
        File newFile = new File(target.getAbsolutePath() + ".tmp");
        PrintWriter output = new PrintWriter(new FileWriter(newFile), true);
        String line = reader.readLine();
        while (line != null) {
            line = line.trim();
            if (!(line.startsWith("//") || line.isEmpty())) {
                Unit unit = Unit.PROGRAM;
                String input = line;
                output.println(input);
                if (input.startsWith("expression") || input.startsWith("program")) {
                    int idx = input.indexOf(' ');
                    unit = Unit.valueOf(input.substring(0, idx).toUpperCase());
                    input = input.substring(idx + 1);
                }
                String expected = reader.readLine();
                yqlplusParser parser = prepareParser(input);
                ParseTree result;
                switch (unit) {
                    case PROGRAM:
                        result = parser.program();
                        break;
                    case EXPRESSION:
                        result = parser.expression(true);
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
                output.println(result.toStringTree());
            } else {
                output.println(line);
            }
            line = reader.readLine();
        }
        output.close();
        return newFile;
    }

    @DataProvider(name = "parsetrees")
    public Object[][] loadParseTrees() throws IOException {
        // read the parse tree sfrom trees.txt
        // // comment
        // blanks skipped
        // [expression|program ]input
        // tree-output
        List<TestParseEntry> entries = parseTestTree(getClass().getResourceAsStream("trees.txt"));
        Object[][] result = new Object[entries.size()][];
        int i = -1;
        for (TestParseEntry entry : entries) {
            result[++i] = new Object[]{entry.unit, entry.input, entry.outputTree};
        }
        return result;
    }

    @DataProvider(name = "programorexpression")
    public Object[][] loadProgramOrExpression() throws IOException {
        // read the parse tree sfrom trees.txt
        // // comment
        // blanks skipped
        // [expression|program ]input
        // tree-output
        List<TestParseEntry> entries = parseTestTree(getClass().getResourceAsStream("programorexpression.txt"));
        Object[][] result = new Object[entries.size()][];
        int i = -1;
        for (TestParseEntry entry : entries) {
            result[++i] = new Object[]{entry.unit, entry.input, entry.outputTree};
        }
        return result;
    }

    @Test(dataProvider = "parsetrees")
    public void testParseTree(Unit unit, String input, String expectedOutput) throws IOException, RecognitionException {
        yqlplusParser parser = prepareParser(input);
        ParseTree output;
        switch (unit) {
            case PROGRAM:
                output = parser.program();
                break;
            case EXPRESSION:
                output = parser.expression(true);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Test(dataProvider = "programorexpression")
    public void testConvertProgramOrExpression(Unit unit, String input, String expectedOutput) throws IOException, RecognitionException {
        String output;
        switch (unit) {
            case PROGRAM:
                output = new ProgramParser().parse("<string", input).toString();
                break;
            case EXPRESSION:
                output = new ProgramParser().parseExpression(input).toString();
                break;
            default:
                throw new IllegalArgumentException();
        }
        Assert.assertEquals(output, expectedOutput);
    }
}
