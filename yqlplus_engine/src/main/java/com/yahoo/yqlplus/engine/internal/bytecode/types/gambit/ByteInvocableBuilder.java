/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.bytecode.ReturnCode;
import com.yahoo.yqlplus.engine.internal.bytecode.UnitGenerator;
import com.yahoo.yqlplus.engine.internal.compiler.MethodGenerator;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BytecodeCastExpression;

public class ByteInvocableBuilder extends ExpressionHandler implements InvocableBuilder {
    private MethodGenerator generator;
    private UnitGenerator unit;

    static class InvocableUnit extends UnitGenerator {
        InvocableUnit(String name, ASMClassSource environment) {
            super(name, environment);
        }
    }

    public ByteInvocableBuilder(ASMClassSource source) {
        super(source);
        this.unit = new InvocableUnit("invocable_" + source.generateUniqueElement(), source);
        this.generator = unit.createStaticMethod("invoke");
        // this isn't going to work when child expressions use things from outer scopes
        body = generator.block();
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        return generator.addArgument(name, type).read();
    }

    @Override
    public Invocable complete(BytecodeExpression result) {
        BytecodeExpression expr = result;
        generator.setReturnType(expr.getType());
        body.add(new BytecodeCastExpression(generator.getReturnType(), expr));
        body.add(new ReturnCode(generator.getReturnType()));
        return generator.createInvocable();
    }
}
