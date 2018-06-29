/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.compiler.code.EngineValueTypeAdapter;
import com.yahoo.yqlplus.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.compiler.runtime.Comparisons;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.code.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class PhysicalExpressionCompilerTest extends CompilingTestBase {

    @Test
    public void requireInOperator() throws Exception {
        Callable<Object> invoker = compileExpression(
                OperatorNode.create(PhysicalExprOperator.IN,
                        constant(1),
                        constant(ImmutableList.of(1, 2, 3)))

        );
        Assert.assertEquals(invoker.call(), true);
    }


    @Test
    public void requireTrivial() throws Exception {
        Callable<Object> invoker = compileExpression(OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.INT32, 1));
        Assert.assertEquals(1, invoker.call());
    }

    @Test
    public void requireAddition() throws Exception {
        Callable<Object> invoker = compileExpression(parseExpression("1 + 2"));
        Assert.assertEquals(3, invoker.call());
    }

    @Test
    public void requireConstantAdd() throws Exception {
        putConstant("foo", 2);
        Callable<Object> invoker = compileExpression(parseExpression("1 + map.foo"));
        Assert.assertEquals(3, invoker.call());
    }

    @Test
    public void requireVariableAdd() throws Exception {
        putConstant("foo", 3);
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.foo"));
        Assert.assertEquals(4, invoker2.call());
    }

    public static class Hat {
        public int hat = 2;
    }

    @Test
    public void requirePropref() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.PROPREF, constant(new Hat()), "hat"));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    @Test
    public void requireIndex() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.INDEX, constant(new Hat()), constant("hat")));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    @Test
    public void requireIndexDynamic() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.INDEX, constant(AnyTypeWidget.getInstance(), new Hat()), constant("hat")));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    @Test
    public void requireMapPropref() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.PROPREF, constant(ImmutableMap.of("hat", 2)), "hat"));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    @Test
    public void requireMapPropefDynamic() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.PROPREF, constant(AnyTypeWidget.getInstance(), ImmutableMap.of("hat", 2)), "hat"));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    @Test
    public void requireMapIndex() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.INDEX, constant(ImmutableMap.of("hat", 2)), constant("hat")));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    @Test
    public void requireMapIndexDynamic() throws Exception {
        putExpr("fancy.hat", OperatorNode.create(PhysicalExprOperator.INDEX, constant(AnyTypeWidget.getInstance(), ImmutableMap.of("hat", 2)), constant("hat")));
        Callable<Object> invoker2 = compileExpression(parseExpression("1 + map.fancy.hat"));
        Assert.assertEquals(3, invoker2.call());
    }

    private Map<String, OperatorNode<PhysicalExprOperator>> lr(OperatorNode<PhysicalExprOperator> left, OperatorNode<PhysicalExprOperator> right) {
        return ImmutableMap.of("left", left, "right", right);
    }

    private Object[] row(Object... row) {
        return row;
    }

    @DataProvider(name = "expressions")
    public Object[][] generateExpressions() {
        // read the parse tree sfrom trees.txt
        // // comment
        // blanks skipped
        // [expression|program ]input
        // tree-output
        Object[] inputs = new Object[]{
                0,
                "",
                "foo",
                true,
                2.2,
                4.1f,
                1L,
                new Hat(),
                new Hat()
        };
        setUp();
        EngineValueTypeAdapter adapter = source.getValueTypeAdapter();
        List<Object[]> items = Lists.newArrayList();
        for (int i = 0; i < inputs.length; ++i) {
            Object leftValue = inputs[i];
            TypeWidget leftType = adapter.inferConstantType(leftValue);
            TypeWidget leftTypeBoxed = leftType.boxed();
            OperatorNode<PhysicalExprOperator> leftExpr = OperatorNode.create(PhysicalExprOperator.CONSTANT, leftType, leftValue);
            OperatorNode<PhysicalExprOperator> leftExprBox = OperatorNode.create(PhysicalExprOperator.CONSTANT, leftTypeBoxed, leftValue);
            OperatorNode<PhysicalExprOperator> nullExpr = OperatorNode.create(PhysicalExprOperator.NULL, (TypeWidget) leftTypeBoxed);
            emitPair(items, leftExpr, nullExpr, false);
            emitPair(items, leftExprBox, nullExpr, false);
            for (int j = i; j < inputs.length; ++j) {
                Object rightValue = inputs[j];
                TypeWidget rightType = adapter.inferConstantType(rightValue);
                TypeWidget rightTypeBoxed = rightType.boxed();
                OperatorNode<PhysicalExprOperator> rightExpr = OperatorNode.create(PhysicalExprOperator.CONSTANT, rightType, rightValue);
                OperatorNode<PhysicalExprOperator> rightExprBox = OperatorNode.create(PhysicalExprOperator.CONSTANT, rightTypeBoxed, rightValue);
                boolean expected = leftValue.equals(rightValue);
                emitPair(items, leftExpr, rightExpr, expected);
                emitPair(items, leftExprBox, rightExpr, expected);
                emitPair(items, leftExprBox, rightExprBox, expected);
                emitPair(items, leftExpr, rightExprBox, expected);
            }
        }
        return items.toArray(new Object[][]{});
    }

    private void emitPair(List<Object[]> items, OperatorNode<PhysicalExprOperator> leftExpr, OperatorNode<PhysicalExprOperator> rightExpr, boolean expected) {
        items.add(row(lr(leftExpr, rightExpr), "map.left = map.right", expected));
        items.add(row(lr(rightExpr, leftExpr), "map.left = map.right", expected));
        items.add(row(lr(leftExpr, rightExpr), "map.left != map.right", !expected));
        items.add(row(lr(rightExpr, leftExpr), "map.left != map.right", !expected));
    }

    @Test(dataProvider = "generateExpressions")
    public void requireExpressions(Map<String, OperatorNode<PhysicalExprOperator>> settings, String expr, Object expected) throws Exception {
        modules.putAll(settings);
        Callable<Object> invoker2 = compileExpression(parseExpression(expr));
        Assert.assertEquals(invoker2.call(), expected);
    }

    @Test
    public void requireUpconvertDouble() throws Exception {
        Object leftValue = 1;
        EngineValueTypeAdapter adapter = source.getValueTypeAdapter();
        TypeWidget leftType = adapter.inferConstantType(leftValue);
        OperatorNode<PhysicalExprOperator> leftExpr = OperatorNode.create(PhysicalExprOperator.CONSTANT, leftType, leftValue);
        Object rightValue = 1.1;
        TypeWidget rightType = adapter.inferConstantType(rightValue);
        TypeWidget rightTypeBoxed = rightType.boxed();
        OperatorNode<PhysicalExprOperator> rightExprBox = OperatorNode.create(PhysicalExprOperator.CONSTANT, rightTypeBoxed, rightValue);
        boolean expected = leftValue.equals(rightValue);
        requireExpressions(lr(leftExpr, rightExprBox), "map.left = map.right", expected);
    }

    @DataProvider(name = "generateComparisons")
    public Object[][] generateComparisons() {
        // read the parse tree sfrom trees.txt
        // // comment
        // blanks skipped
        // [expression|program ]input
        // tree-output
        return new Object[][]{
                {"1", "0"},
                {"0", "1"},
                {"0", "0"},
                {1, 0},
                {0, 1},
                {0, 0},
                {1.0, 0.0},
                {0.0, 1.0},
                {0.0, 0.0},
                {1.0f, 0.0f},
                {0.0f, 1.0f},
                {0.0f, 0.0f},
        };

    }

    String getOperator(BinaryComparison op) {
        switch (op) {
            case LT:
                return "<";
            case LTEQ:
                return "<=";
            case GT:
                return ">";
            case GTEQ:
                return ">=";
        }
        throw new UnsupportedOperationException();
    }

    @Test(dataProvider = "generateComparisons")
    public void requireComparisons(Comparable left, Comparable right) throws Exception {
        for (BinaryComparison op : BinaryComparison.values()) {
            String expr = "map.left " + getOperator(op) + " map.right";
            boolean expected = Comparisons.INSTANCE.compare(op, left, right);
            setUp();
            putConstant("left", left);
            putConstant("right", right);
            Callable<Object> invoker2 = compileExpression(parseExpression(expr));
            Assert.assertEquals(invoker2.call(), expected);
        }
        setUp();
        int val = Comparisons.INSTANCE.compare(left, right);
        Callable<Object> invoker3 = compileExpression(OperatorNode.create(PhysicalExprOperator.COMPARE,
                constant(left),
                constant(right)));
        Assert.assertEquals(invoker3.call(), val);
    }

    @Test
    public void requireOne() throws Exception {
        requireComparisons("1", "0");
    }

}
