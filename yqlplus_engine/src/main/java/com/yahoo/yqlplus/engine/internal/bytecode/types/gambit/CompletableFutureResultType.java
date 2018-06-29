/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.PromiseAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeWidget;
import org.objectweb.asm.Type;

import java.util.concurrent.CompletableFuture;

public class CompletableFutureResultType extends BaseTypeWidget {
    private final TypeWidget valueType;

    public CompletableFutureResultType(TypeWidget valueType) {
        super(Type.getType(CompletableFuture.class));
        this.valueType = valueType;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.PROMISE;
    }


    @Override
    public boolean isPromise() {
        return true;
    }

    @Override
    public PromiseAdapter getPromiseAdapter() {
        return new FuturePromiseAdapter(valueType);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return true;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(final ProgramValueTypeAdapter typeAdapter) {
        return new UnificationAdapter() {
            @Override
            public TypeWidget unify(TypeWidget other) {
                PromiseAdapter promiseAdapter = other.getPromiseAdapter();
                return new CompletableFutureResultType(typeAdapter.unifyTypes(valueType, promiseAdapter.getResultType()));
            }
        };
    }

}
