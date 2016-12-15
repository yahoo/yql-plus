/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;

class ConstructingCallableInvocable implements CallableInvocable {
    private final TypeWidget resultType;
    private final GambitCreator.Invocable parent;

    ConstructingCallableInvocable(TypeWidget resultType, GambitCreator.Invocable parent) {
        this.resultType = resultType;
        this.parent = parent;
    }

    @Override
    public TypeWidget getResultType() {
        return resultType;
    }

    @Override
    public TypeWidget getReturnType() {
        return parent.getReturnType();
    }

    @Override
    public List<TypeWidget> getArgumentTypes() {
        return parent.getArgumentTypes();
    }

    @Override
    public CallableInvocable prefix(BytecodeExpression... arguments) {
        return new ConstructingCallableInvocable(resultType, parent.prefix(arguments));
    }

    @Override
    public CallableInvocable prefix(List<BytecodeExpression> arguments) {
        return new ConstructingCallableInvocable(resultType, parent.prefix(arguments));
    }

    @Override
    public BytecodeExpression invoke(Location loc, List<BytecodeExpression> args) {
        return parent.invoke(loc, args);
    }

    @Override
    public BytecodeExpression invoke(Location loc, BytecodeExpression... args) {
        return parent.invoke(loc, args);
    }
}
