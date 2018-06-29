/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.types.YQLType;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;


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

    @Override
    public CallableInvocableBuilder createInvocableCallable() {
        return new CallableBuilder(source, this);
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

    public TypeWidget adapt(Class<?> type, boolean nullable, TypeWidget... typeArguments) {
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
    public TypeWidget resultTypeFor(TypeWidget valueType) {
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
