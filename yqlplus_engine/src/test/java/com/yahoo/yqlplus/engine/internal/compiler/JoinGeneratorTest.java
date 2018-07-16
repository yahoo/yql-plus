/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.compiler.code.ASMClassSource;
import com.yahoo.yqlplus.engine.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.engine.compiler.code.GambitCreator;
import com.yahoo.yqlplus.engine.compiler.code.GambitScope;
import com.yahoo.yqlplus.engine.compiler.code.GambitSource;
import com.yahoo.yqlplus.engine.compiler.code.ObjectBuilder;
import com.yahoo.yqlplus.engine.compiler.runtime.JoinTask;
import com.yahoo.yqlplus.engine.internal.generate.JoinGenerator;
import com.yahoo.yqlplus.language.parser.Location;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JoinGeneratorTest {

    public static class Handle {
        int count = 0;
        boolean forked = false;

        public void incr() {
            ++count;
        }

        public void add(int a, int b) {
            count += a + b;
        }
    }

    private ASMClassSource source;
    private GambitScope scope;

    @BeforeMethod
    public void setUp() {
        source = new ASMClassSource();
        scope = new GambitSource(source);
    }

    @Test
    public void requireJoin() throws Throwable {
        Handle handle = new Handle();
        final BytecodeExpression handleExpr = source.constant(source.adaptInternal(Handle.class), handle);
        ObjectBuilder program = scope.createObject();
        JoinGenerator joinGenerator = new JoinGenerator(program.type(), scope, 2);
        final GambitCreator.ScopeBuilder body = joinGenerator.getBody();
        body.exec(body.invokeExact(Location.NONE, "incr", Handle.class, BaseTypeAdapter.VOID, handleExpr));
        scope.build();
        Class<?> programClazz = scope.getObjectClass(program);
        Class<? extends JoinTask> clazz = (Class<? extends JoinTask>) scope.getObjectClass(joinGenerator.getBuilder());
        JoinTask task = clazz.getConstructor(programClazz, TaskContext.class).newInstance(programClazz.newInstance(), TaskContext.builder().build());
        task.run();
        Assert.assertEquals(0, handle.count);
        task.run();
        Assert.assertEquals(1, handle.count);
    }
}
