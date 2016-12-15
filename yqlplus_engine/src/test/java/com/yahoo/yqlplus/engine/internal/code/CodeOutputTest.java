/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class CodeOutputTest {

    @Test
    public void testPrint() {
        CodeOutput output = new CodeOutput();
        output.print("foo");
        output.print("%d", 1);
        output.println("bar%d");
        output.println("baz%d", 1);
        Assert.assertEquals(output.toString(), "foo1bar%d\nbaz1\n");
    }

    @Test
    public void testIndent() {
        CodeOutput output = new CodeOutput();
        output.println("a");
        output.indent();
        output.println("b");
        output.dedent();
        output.println("c");
        Assert.assertEquals(output.toString(), "a\n   b\nc\n");
    }

    @Test
    public void testBlock() {
        CodeOutput output = new CodeOutput();
        output.println("a");
        output.indent();
        output.println("b");
        CodeBlock block = output.block();
        output.dedent();
        output.println("c");

        block.println("{");
        block.indent();
        block.println("a2");
        CodeBlock more = block.block();
        block.println("a3");
        block.dedent();
        block.println("}");

        more.println("d1");

        Assert.assertEquals(output.toString(), "a\n   b\n   {\n      a2\n      d1\n      a3\n   }\nc\n");
    }

    @Test
    public void testGensym() {
        CodeOutput output = new CodeOutput();
        output.println(output.gensym());
        output.indent();
        CodeBlock block = output.block();
        output.dedent();

        block.println(block.gensym());

        Assert.assertEquals(output.toString(), "sym0\n   sym1\n");

    }

    @Test
    public void testDynamicChunk() {
        CodeOutput output = new CodeOutput();
        output.println("a");
        output.indent();
        output.add(new CodeRenderable() {
            @Override
            public void render(CodePrinter output) {
                output.println("foo");
            }
        });
        output.println("b");
        Assert.assertEquals(output.toString(), "a\n   foo\n   b\n");
    }

}
