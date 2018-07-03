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
    private final String methodName;
    private final TypeWidget functionInterface;
    private final List<TypeWidget> factoryArguments;
    private final List<TypeWidget> arguments;
    private final TypeWidget methodResult;
    private boolean allowFactoryArguments = true;

    public LambdaBuilder(ASMClassSource source, TypeWidget functionInterface, String methodName, TypeWidget methodResult) {
        super(source);
        this.generator = source.createInvocableMethod(methodName);
        this.call = new MethodAdapter(source, generator);
        body = call.getCode();
        this.methodName = methodName;
        this.functionInterface = functionInterface;
        this.arguments = Lists.newArrayList();
        this.factoryArguments = Lists.newArrayList();
        this.methodResult = methodResult;
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        Preconditions.checkState(allowFactoryArguments, "Cannot add any further lambda factory arguments once addLambdaArgument called");
        this.factoryArguments.add(type);
        return this.call.addArgument(name, type);
    }

    public BytecodeExpression addLambdaArgument(String name, TypeWidget type) {
        allowFactoryArguments = false;
        this.arguments.add(type);
        return this.call.addArgument(name, type);
    }

    @Override
    public LambdaInvocable complete(BytecodeExpression result) {
        call.exit(call.cast(Location.NONE, this.methodResult, result));
        return  source.createLambdaFactory(generator,  factoryArguments, functionInterface, methodResult, methodName, arguments);
    }

    @Override
    public LambdaInvocable exit() {
        return source.createLambdaFactory(generator,  factoryArguments, functionInterface, methodResult, methodName, arguments);
    }

    @Override
    public MethodHandle getFactory() throws Throwable {
        ASMClassSource.LambdaFactoryCallable factory = source.createLambdaFactory(generator,  factoryArguments, functionInterface, methodResult, methodName, arguments);
        return factory.invoker();
    }
}
