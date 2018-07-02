/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.language.parser.Location;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.Callable;

import static java.lang.invoke.MethodType.methodType;

public class CallableBuilder extends ExpressionHandler implements CallableInvocableBuilder {
    private MethodGenerator generator;
    private ObjectBuilder.MethodBuilder call;

    public CallableBuilder(ASMClassSource source) {
        super(source);
        this.generator = source.createInvocableMethod("call");
        this.call = new MethodAdapter(source, generator);
        body = call.getCode().block();
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        return this.call.addArgument(name, type);
    }

    @Override
    public CallableInvocable complete(BytecodeExpression result) {
        call.exit(call.cast(Location.NONE, AnyTypeWidget.getInstance(), result));
        return new ConstructingCallableInvocable(result.getType(), source.callLambdaFactory(generator, Callable.class, "call", methodType(Object.class)));
    }

    @Override
    public MethodHandle getFactory() throws Throwable {
        return source.getLambdaFactory(generator, Callable.class, "call", methodType(Object.class));
    }


}
