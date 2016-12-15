/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;

import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;

public interface EngineValueTypeAdapter extends ValueTypeAdapter {
    ASMClassSource getClassGenerator();

    BytecodeExpression constant(TypeWidget type, Object value);

    BytecodeExpression constant(Object value);

    TypeWidget unifyTypes(TypeWidget left, TypeWidget right);

    TypeWidget unifyTypes(Iterable<TypeWidget> types);

    TypeWidget adapt(YQLType type);

    TypeWidget inferConstantType(Object constantValue);


    TypeWidget adaptInternal(TypeLiteral<?> typeLiteral, boolean nullable);
}
