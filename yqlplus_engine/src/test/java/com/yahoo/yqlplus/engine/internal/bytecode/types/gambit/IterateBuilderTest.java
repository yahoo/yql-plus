/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.language.parser.Location;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;


public class IterateBuilderTest extends GambitSourceTestBase {
    public class Handler implements Iterable<Integer> {
        private List<Integer> ints;
        private int total;

        public Handler(List<Integer> ints) {
            this.ints = ints;
        }

        @Override
        public Iterator<Integer> iterator() {
            return ints.iterator();
        }

        public void add(Integer item) {
            total += item;
        }
    }

    @Test
    public void requireBuilder() throws Exception {
        ObjectBuilder o = source.createObject();
        o.implement(Runnable.class);
        o.addParameter("handler", source.adapt(Handler.class, false));
        ObjectBuilder.MethodBuilder builder = o.method("run");
        GambitCreator.IterateBuilder loop = builder.iterate(builder.local("handler"));
        loop.next(loop.eq(Location.NONE, loop.getItem(), source.constant(1)));
        loop.abort(loop.eq(Location.NONE, loop.getItem(), source.constant(5)));
        loop.exec(loop.invokeExact(Location.NONE, "add", Handler.class, BaseTypeAdapter.VOID, builder.local("handler"), loop.getItem()));
        builder.exec(loop.build());
        builder.exit();
        source.build();
        Class<? extends Runnable> out = (Class<? extends Runnable>) source.getObjectClass(o);
        Handler h1 = new Handler(ImmutableList.of(1, 2, 3, 4, 5, 6));
        Runnable z = out.getConstructor(Handler.class).newInstance(h1);
        z.run();
        // skip 1, 2 + 3 + 4; abort at 5
        Assert.assertEquals(h1.total, 9);
    }
}
