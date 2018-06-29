/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.compiler.code.ObjectBuilder;
import com.yahoo.yqlplus.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.language.parser.Location;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;


public class ObjectBuilderTest extends GambitSourceTestBase {
    @Test
    public void requireBuilder() throws Exception {
        ObjectBuilder o = source.createObject();
        o.implement(Callable.class);
        o.addParameter("hi", BaseTypeAdapter.INT32);
        ObjectBuilder.MethodBuilder builder = o.method("call");
        builder.exit(builder.cast(Location.NONE, BaseTypeAdapter.ANY, builder.local("hi")));
        source.build();
        Class<? extends Callable> out = (Class<? extends Callable>) source.getObjectClass(o);
        Callable<Object> z = out.getConstructor(int.class).newInstance(1);
        Assert.assertEquals(z.call(), 1);
    }
}
