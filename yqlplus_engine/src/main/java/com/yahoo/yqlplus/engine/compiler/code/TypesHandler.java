/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.types.YQLType;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;


public class TypesHandler implements GambitTypes {
    protected final ASMClassSource source;

    @Override
    public EngineValueTypeAdapter getValueTypeAdapter() {
        return source.getValueTypeAdapter();
    }

    @Override
    public InvocableBuilder createInvocable() {
        return new ByteInvocableBuilder(source);
    }

    private FunctionalInterfaceContract functional(Class<?> clazz, String methodName, Class<?> returnType, boolean nullable, Class<?> ...argumentTypes) {
        List<TypeWidget> arguments = Lists.newArrayListWithExpectedSize(argumentTypes == null ? 0 : argumentTypes.length);
        if(argumentTypes != null) {
            for(Class<?> argType : argumentTypes) {
                arguments.add(adapt(argType, true));
            }
        }
        return new FunctionalInterfaceContract(adapt(clazz, false), methodName, adapt(returnType, nullable), arguments);
    }

    @Override
    public LambdaFactoryBuilder createInvocableCallable() {
        return new LambdaBuilder(source, functional(Callable.class, "call", Object.class, true));
    }

    @Override
    public LambdaFactoryBuilder createLambdaBuilder(Class<?> clazz, String methodName, Class<?> returnType, boolean nullable, Class<?> ...argumentTypes) {
        FunctionalInterfaceContract contract = functional(clazz, methodName, returnType, nullable, argumentTypes);
        return new LambdaBuilder(source, contract);
    }

    public TypesHandler(ASMClassSource source) {
        this.source = source;
    }

    public TypeWidget adapt(YQLType type) {
        return source.adapt(type);
    }

    public TypeWidget adapt(Type type, boolean nullable) {
        TypeWidget result = source.adaptInternal(type);
        if (!nullable) {
            return NotNullableTypeWidget.create(result);
        }
        return result;
    }

    @Override
    public TypeWidget adapt(org.objectweb.asm.Type type, boolean nullable) {
        TypeWidget result = source.adaptInternal(type);
        if (!nullable) {
            return NotNullableTypeWidget.create(result);
        }
        return result;
    }

    public TypeWidget unify(List<? extends TypeWidget> types) {
        return source.getValueTypeAdapter().unifyTypes((Iterable<TypeWidget>) types);
    }

    @Override
    public TypeWidget unify(TypeWidget left, TypeWidget right) {
        return source.getValueTypeAdapter().unifyTypes(left, right);
    }

    public BytecodeExpression constant(TypeWidget type, Object value) {
        return source.constant(type, value);
    }

    public BytecodeExpression constant(Object value) {
        return source.constant(value);
    }

    public BytecodeExpression nullFor(TypeWidget type) {
        return new NullExpr(NullableTypeWidget.create(type));
    }

    @Override
    public ObjectBuilder createObject() {
        return new GambitUnit("c_" + source.generateUniqueElement(), source);
    }


    @Override
    public ObjectBuilder createObject(Class<?> superClass, TypeWidget... typeArguments) {
        return new GambitUnit("obj_" + source.generateUniqueElement(), superClass, source);
    }

    @Override
    public ResultAdapter resultTypeFor(TypeWidget valueType) {
        return source.resultTypeFor(valueType);
    }

    @Override
    public GambitCreator.Invocable findExactInvoker(Class<?> owner, String methodName, TypeWidget returnType, List<TypeWidget> argumentTypes) {
        return ExactInvocation.exactInvoke(owner.isInterface() ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL, methodName, adapt(owner, false), returnType, argumentTypes);
    }

    @Override
    public GambitCreator.Invocable findStaticInvoker(Class<?> owner, String methodName, TypeWidget returnType, List<TypeWidget> argumentTypes) {
        return ExactInvocation.exactInvoke(Opcodes.INVOKESTATIC, methodName, adapt(owner, false), returnType, argumentTypes);
    }

    @Override
    public GambitCreator.Invocable findExactInvoker(Class<?> owner, String methodName, TypeWidget returnType, TypeWidget... argumentTypes) {
        return findExactInvoker(owner, methodName, returnType, argumentTypes != null ? Arrays.asList(argumentTypes) : ImmutableList.of());
    }

    @Override
    public GambitCreator.Invocable findStaticInvoker(Class<?> owner, String methodName, TypeWidget returnType, TypeWidget... argumentTypes) {
        return findStaticInvoker(owner, methodName, returnType, argumentTypes != null ? Arrays.asList(argumentTypes) : ImmutableList.of());
    }

    @Override
    public GambitCreator.Invocable findExactInvoker(Class<?> owner, String methodName, TypeWidget returnType, Class<?>... argumentTypes) {
        return findExactInvoker(owner, methodName, returnType, adaptAll(argumentTypes));
    }

    @Override
    public GambitCreator.Invocable findStaticInvoker(Class<?> owner, String methodName, TypeWidget returnType, Class<?>... argumentTypes) {
        return findStaticInvoker(owner, methodName, returnType, adaptAll(argumentTypes));
    }

    private List<TypeWidget> adaptAll(Class<?>[] argumentTypes) {
        if (argumentTypes == null || argumentTypes.length == 0) {
            return ImmutableList.of();
        }
        List<TypeWidget> widgets = Lists.newArrayListWithCapacity(argumentTypes.length);
        for (Class<?> clazz : argumentTypes) {
            widgets.add(adapt(clazz, true));
        }
        return widgets;
    }

    @Override
    public StructBuilder createStruct() {
        return new GambitStructBuilder(source);
    }
}
