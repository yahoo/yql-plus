/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

import java.util.List;

public interface GambitTypes {
    ProgramValueTypeAdapter getValueTypeAdapter();

    StructBuilder createStruct();

    ObjectBuilder createObject();

    ObjectBuilder createObject(Class<?> superClass, TypeWidget... typeArguments);

    InvocableBuilder createInvocable();

    CallableInvocableBuilder createInvocableCallable();

    TypeWidget adapt(YQLType type);

    TypeWidget adapt(java.lang.reflect.Type type, boolean nullable);

    TypeWidget adapt(Class<?> type, boolean nullable, TypeWidget... typeArguments);

    TypeWidget unify(List<? extends TypeWidget> types);

    TypeWidget unify(TypeWidget left, TypeWidget right);

    TypeWidget resultTypeFor(TypeWidget valueType);

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
