/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.parser.Location;

import java.lang.invoke.MethodHandle;
import java.util.List;

public class LambdaBuilder extends ExpressionHandler implements LambdaFactoryBuilder {
    private final MethodGenerator generator;
    private final ObjectBuilder.MethodBuilder call;
    private final FunctionalInterfaceContract contract;

    public LambdaBuilder(ASMClassSource source, FunctionalInterfaceContract contract) {
        super(source);
        this.contract = contract;
        this.generator = source.createInvocableMethod(contract.methodName);
        this.call = new MethodAdapter(source, generator);
        body = call.getCode();
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        return this.call.addArgument(name, type);
    }

    @Override
    public LambdaInvocable complete(BytecodeExpression result) {
        call.exit(result);
        return source.createLambdaFactory(generator, contract);
    }

    @Override
    public LambdaInvocable exit() {
        return source.createLambdaFactory(generator,  contract);
    }

    @Override
    public MethodHandle getFactory() throws Throwable {
        ASMClassSource.LambdaFactoryCallable factory = source.createLambdaFactory(generator, contract);
        return factory.invoker();
    }
}
