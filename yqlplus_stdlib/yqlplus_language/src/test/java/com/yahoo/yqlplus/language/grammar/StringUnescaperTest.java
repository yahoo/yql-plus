/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.grammar;

import com.yahoo.yqlplus.language.internal.ast.StringUnescaper;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class StringUnescaperTest {
    @Test
    public void testUnescapeBase() {
        Assert.assertEquals("foobar", StringUnescaper.unquote("'foobar'"));
        Assert.assertEquals("foobar'", StringUnescaper.unquote("'foobar\''"));
        Assert.assertEquals("foo'bar", StringUnescaper.unquote("'foo\'bar'"));
        Assert.assertEquals("''foobar", StringUnescaper.unquote("'\\'\\'foobar'"));
    }

    @Test
    public void testUnescapeUnicode() {
        Assert.assertEquals("foo\u0001bar", StringUnescaper.unquote("'foo\\u0001bar'"));
        Assert.assertEquals("foo\u0001bar\u0001", StringUnescaper.unquote("'foo\\u0001bar\\u0001'"));
    }

    @Test
    public void testUnescapeOctal() {
        Assert.assertEquals("foo\1bar", StringUnescaper.unquote("'foo\\1bar'"));
        Assert.assertEquals("foo\01bar", StringUnescaper.unquote("'foo\\01bar'"));
        Assert.assertEquals("foo\001bar", StringUnescaper.unquote("'foo\\001bar'"));
        Assert.assertEquals("foobar\1", StringUnescaper.unquote("'foobar\\1'"));
        Assert.assertEquals("foobar\65", StringUnescaper.unquote("'foobar\\65'"));
        Assert.assertEquals("foobar\001", StringUnescaper.unquote("'foobar\\001'"));
        Assert.assertEquals("foobar\001a", StringUnescaper.unquote("'foobar\\001a'"));
    }

    @Test
    public void testEscape() {
        Assert.assertEquals("'foobar'", StringUnescaper.escape("foobar"));
        Assert.assertEquals("'foo\\'bar'", StringUnescaper.escape("foo'bar"));
    }
}
