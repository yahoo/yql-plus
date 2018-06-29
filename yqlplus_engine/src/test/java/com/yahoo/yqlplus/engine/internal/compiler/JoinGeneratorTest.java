/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.yahoo.yqlplus.api.types.Annotations;
import com.yahoo.yqlplus.compiler.code.JoinGenerator;
import com.yahoo.yqlplus.compiler.code.JoinTask;
import com.yahoo.yqlplus.compiler.generate.ASMClassSource;
import com.yahoo.yqlplus.compiler.generate.ASMClassSourceModule;
import com.yahoo.yqlplus.compiler.exprs.LocalVarExpr;
import com.yahoo.yqlplus.compiler.generate.GambitCreator;
import com.yahoo.yqlplus.compiler.generate.GambitScope;
import com.yahoo.yqlplus.compiler.generate.GambitSource;
import com.yahoo.yqlplus.compiler.generate.ObjectBuilder;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
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
        source = Guice.createInjector(new ASMClassSourceModule()).getInstance(ASMClassSource.class);
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
        JoinTask task = clazz.getConstructor(programClazz).newInstance(programClazz.newInstance());
        task.run();
        Assert.assertEquals(0, handle.count);
        task.run();
        Assert.assertEquals(1, handle.count);
    }

    @Test
    public void requireJoinArgumented() throws Throwable {
        Handle handle = new Handle();
        final BytecodeExpression handleExpr = source.constant(source.adaptInternal(Handle.class), handle);
        ObjectBuilder program = scope.createObject();
        JoinGenerator joinGenerator = new JoinGenerator(program.type(), scope, 2);
        joinGenerator.addValue("a", BaseTypeAdapter.INT32);
        joinGenerator.addValue("b", BaseTypeAdapter.INT32);
        final GambitCreator.ScopeBuilder body = joinGenerator.getBody();
        body.exec(body.invokeExact(Location.NONE, "add", Handle.class, BaseTypeAdapter.VOID, handleExpr, body.local("a"), body.local("b")));

        ObjectBuilder.FieldBuilder joinField = program.finalField("join", joinGenerator.getType().construct(new LocalVarExpr(program.type(), "this")));
        BytecodeExpression joinExpr = joinField.get(new LocalVarExpr(program.type(), "this"));

        program.addParameter("a", BaseTypeAdapter.INT32);
        program.addParameter("b", BaseTypeAdapter.INT32);

        OperatorValue v1 = new OperatorValue(true, Annotations.EMPTY);
        v1.setName("a");
        OperatorValue v2 = new OperatorValue(true, Annotations.EMPTY);
        v2.setName("b");
        ObjectBuilder.MethodBuilder part1 = program.method("part1");
        part1.exec(part1.invokeExact(Location.NONE, "run", Runnable.class, BaseTypeAdapter.VOID, joinGenerator.createRunnable(part1, joinExpr, ImmutableList.of(v1))));
        part1.exit();

        ObjectBuilder.MethodBuilder part2 = program.method("part2");
        part2.exec(part2.invokeExact(Location.NONE, "run", Runnable.class, BaseTypeAdapter.VOID, joinGenerator.createRunnable(part2, joinExpr, ImmutableList.of(v2))));
        part2.exit();

        scope.build();
        Class<?> programClazz = scope.getObjectClass(program);
        Object pgm = programClazz.getConstructor(int.class, int.class).newInstance(1, 2);
        programClazz.getMethod("part1").invoke(pgm);
        Assert.assertEquals(handle.count, 0);
        programClazz.getMethod("part2").invoke(pgm);
        Assert.assertEquals(handle.count, 3);
    }
}
