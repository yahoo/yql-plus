/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.compiler.generate.GambitCreator;
import com.yahoo.yqlplus.compiler.generate.ObjectBuilder;
import com.yahoo.yqlplus.compiler.generate.ScopedBuilder;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import com.yahoo.yqlplus.language.parser.Location;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class CatchHandlerTest extends GambitSourceTestBase {
    public class Handler {
        int result;
        boolean failed;
        Throwable failure;

        public void ok(int a) {
            result = a;
        }

        public void fail(Throwable t) {
            failed = true;
            failure = t;
        }

        public int doit(boolean ok, int result) {
            if (!ok) {
                throw new RuntimeException("fancy");
            } else {
                return result;
            }
        }
    }

    private Class<? extends Runnable> createRunnable() throws IOException, ClassNotFoundException {
        ObjectBuilder o = source.createObject();
        o.implement(Runnable.class);
        o.addParameter("handler", source.adapt(Handler.class, false));
        o.addParameter("ok", BaseTypeAdapter.BOOLEAN);
        o.addParameter("code", BaseTypeAdapter.INT32);
        ObjectBuilder.MethodBuilder builder = o.method("run");
        GambitCreator.CatchBuilder catchBuilder = builder.tryCatchFinally();
        ScopedBuilder body = catchBuilder.body();
        BytecodeExpression inv = builder.invokeExact(Location.NONE, "doit", Handler.class, BaseTypeAdapter.INT32,
                body.local("handler"), body.local("ok"), body.local("code"));
        body.exec(builder.invokeExact(Location.NONE, "ok", Handler.class, BaseTypeAdapter.VOID, body.local("handler"), inv));

        ScopedBuilder handler = catchBuilder.on("$e", RuntimeException.class);
        handler.exec(handler.invokeExact(Location.NONE, "fail", Handler.class, BaseTypeAdapter.VOID, handler.local("handler"),
                handler.cast(Location.NONE, source.adapt(Throwable.class, false), handler.local("$e"))));
        builder.exec(catchBuilder.build());
        builder.exit();
        source.build();
        return (Class<? extends Runnable>) source.getObjectClass(o);
    }


    @Test
    public void requireSuccess() throws Exception {
        Class<? extends Runnable> out = createRunnable();
        Handler h1 = new Handler();
        Runnable z = out.getConstructor(Handler.class, boolean.class, int.class).newInstance(h1, true, 1);
        z.run();
        Assert.assertEquals(h1.result, 1);
        Assert.assertEquals(h1.failed, false);
    }


    @Test
    public void requireFailure() throws Exception {
        Class<? extends Runnable> out = createRunnable();
        Handler h2 = new Handler();
        Runnable z2 = out.getConstructor(Handler.class, boolean.class, int.class).newInstance(h2, false, 1);
        z2.run();
        Assert.assertEquals(h2.result, 0);
        Assert.assertEquals(h2.failed, true);
        Assert.assertEquals(h2.failure.getMessage(), "fancy");
    }
}
