/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.backends.java;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.compiler.runtime.Chooser;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class ChooserTest {
    @Test
    public void testChoose1of1() {
        Chooser<String> chooser = Chooser.create();
        List<String> input = ImmutableList.of("A");
        List<List<String>> output = ImmutableList.copyOf(chooser.chooseN(1, input));
        Assert.assertEquals(output.size(), 1);
        Assert.assertEquals(output, ImmutableList.of(ImmutableList.of("A")));
    }

    @Test
    public void testChoose2of2() {
        Chooser<String> chooser = Chooser.create();
        List<String> input = ImmutableList.of("A", "B");
        List<List<String>> output = ImmutableList.copyOf(chooser.chooseN(2, input));
        Assert.assertEquals(output.size(), 1);
        Assert.assertEquals(output, ImmutableList.of(ImmutableList.of("A", "B")));
    }

    @Test
    public void testChoose1of2() {
        Chooser<String> chooser = Chooser.create();
        List<String> input = ImmutableList.of("A", "B");
        List<List<String>> output = ImmutableList.copyOf(chooser.chooseN(1, input));
        Assert.assertEquals(output.size(), 2);
        Assert.assertEquals(output, ImmutableList.of(ImmutableList.of("A"), ImmutableList.of("B")));
    }

    @Test
    public void testChoose2of3() {
        Chooser<String> chooser = Chooser.create();
        List<String> input = ImmutableList.of("A", "B", "C");
        List<List<String>> output = ImmutableList.copyOf(chooser.chooseN(2, input));
        Assert.assertEquals(output, ImmutableList.of(
                ImmutableList.of("A", "B"),
                ImmutableList.of("A", "C"),
                ImmutableList.of("B", "C")));
    }

    @Test
    public void testChoose3of4() {
        Chooser<String> chooser = Chooser.create();
        List<String> input = ImmutableList.of("A", "B", "C", "D");
        List<List<String>> output = ImmutableList.copyOf(chooser.chooseN(3, input));
        Assert.assertEquals(output, ImmutableList.of(
                ImmutableList.of("A", "B", "C"),
                ImmutableList.of("A", "B", "D"),
                ImmutableList.of("A", "C", "D"),
                ImmutableList.of("B", "C", "D")));
    }

    @Test
    public void testChoose2of1() {
        Chooser<String> chooser = Chooser.create();
        List<String> input = ImmutableList.of("A");
        List<List<String>> output = ImmutableList.copyOf(chooser.chooseN(2, input));
        Assert.assertEquals(output, ImmutableList.of());
    }
}
