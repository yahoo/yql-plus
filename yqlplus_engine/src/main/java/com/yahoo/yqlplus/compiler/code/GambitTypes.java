/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.api.types.YQLType;

import java.util.List;

public interface GambitTypes {
    EngineValueTypeAdapter getValueTypeAdapter();

    StructBuilder createStruct();

    ObjectBuilder createObject();

    ObjectBuilder createObject(Class<?> superClass, TypeWidget... typeArguments);

    InvocableBuilder createInvocable();

    LambdaFactoryBuilder createInvocableCallable();
    LambdaFactoryBuilder createLambdaBuilder(Class<?> clazz, String methodName, Class<?> returnType, boolean nullable, Class<?> ...argumentTypes);

    TypeWidget adapt(YQLType type);

    TypeWidget adapt(java.lang.reflect.Type type, boolean nullable);

    TypeWidget unify(List<? extends TypeWidget> types);

    TypeWidget unify(TypeWidget left, TypeWidget right);

    ResultAdapter resultTypeFor(TypeWidget valueType);

    BytecodeExpression constant(TypeWidget type, Object value);

    BytecodeExpression constant(Object value);

    BytecodeExpression nullFor(TypeWidget type);

    GambitCreator.Invocable findExactInvoker(Class<?> owner, String methodName, TypeWidget returnType, List<TypeWidget> argumentTypes);

    GambitCreator.Invocable findStaticInvoker(Class<?> owner, String methodName, TypeWidget returnType, List<TypeWidget> argumentTypes);

    GambitCreator.Invocable findExactInvoker(Class<?> owner, String methodName, TypeWidget returnType, TypeWidget... argumentTypes);

    GambitCreator.Invocable findStaticInvoker(Class<?> owner, String methodName, TypeWidget returnType, TypeWidget... argumentTypes);

    GambitCreator.Invocable findExactInvoker(Class<?> owner, String methodName, TypeWidget returnType, Class<?>... argumentTypes);

    GambitCreator.Invocable findStaticInvoker(Class<?> owner, String methodName, TypeWidget returnType, Class<?>... argumentTypes);
}
