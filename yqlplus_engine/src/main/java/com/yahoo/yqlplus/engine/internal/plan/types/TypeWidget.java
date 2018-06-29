/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ResultAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ComparisonAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.PropertyAdapter;
import org.objectweb.asm.Type;

import java.util.List;

public interface TypeWidget {

    Type getJVMType();

    boolean isPrimitive();

    BytecodeExpression construct(BytecodeExpression... arguments);

    // a field-based deserialize?

    Coercion coerceTo(BytecodeExpression source, TypeWidget target);

    TypeWidget boxed();

    TypeWidget unboxed();

    BytecodeExpression invoke(BytecodeExpression target, String methodName, List<BytecodeExpression> arguments);

    BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments);

    ComparisonAdapter getComparisionAdapter();

    YQLCoreType getValueCoreType();

    boolean isNullable();

    boolean hasProperties();

    PropertyAdapter getPropertyAdapter();

    boolean isIndexable();

    IndexAdapter getIndexAdapter();

    boolean isIterable();

    IterateAdapter getIterableAdapter();

    boolean isPromise();

    PromiseAdapter getPromiseAdapter();

    boolean isResult();

    ResultAdapter getResultAdapter();

    String getTypeName();

    boolean isAssignableFrom(TypeWidget type);

    boolean equals(Object other);

    boolean hasUnificationAdapter();

    UnificationAdapter getUnificationAdapter(ProgramValueTypeAdapter typeAdapter);

    interface UnificationAdapter {
        TypeWidget unify(TypeWidget other);
    }

    final class Coercion {
        public final int cost;
        public final BytecodeExpression expr;

        public Coercion(int cost, BytecodeExpression expr) {
            this.cost = cost;
            this.expr = expr;
        }
    }
}
