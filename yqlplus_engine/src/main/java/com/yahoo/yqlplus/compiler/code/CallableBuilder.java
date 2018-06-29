/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.language.parser.Location;

import java.util.concurrent.Callable;

public class CallableBuilder extends ExpressionHandler implements CallableInvocableBuilder {
    private ObjectBuilder.MethodBuilder call;
    private ObjectBuilder unit;

    public CallableBuilder(ASMClassSource source, GambitTypes scope) {
        super(source);
        this.unit = scope.createObject();
        this.unit.implement(Callable.class);
        this.call = unit.method("call");
        body = call.getCode().block();
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        unit.addParameter(name, type);
        return body.getLocal(name);
    }

    @Override
    public CallableInvocable complete(BytecodeExpression result) {
        call.exit(call.cast(Location.NONE, AnyTypeWidget.getInstance(), result));
        return new ConstructingCallableInvocable(result.getType(), CallableBuilder.this.unit.getConstructor().invoker());
    }

    @Override
    public ObjectBuilder builder() {
        return unit;
    }

    @Override
    public TypeWidget type() {
        return unit.type();
    }

}
