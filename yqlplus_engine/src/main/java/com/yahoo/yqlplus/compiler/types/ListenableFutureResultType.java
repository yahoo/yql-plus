/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.compiler.code.EngineValueTypeAdapter;
import com.yahoo.yqlplus.compiler.code.PromiseAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Type;

public class ListenableFutureResultType extends BaseTypeWidget {
    private final TypeWidget valueType;

    public ListenableFutureResultType(TypeWidget valueType) {
        super(Type.getType(ListenableFuture.class));
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
        // TODO: a listenablefuture promise adapter which supports async/callback based resolution
        return new FuturePromiseAdapter(valueType);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return true;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(final EngineValueTypeAdapter typeAdapter) {
        return new UnificationAdapter() {
            @Override
            public TypeWidget unify(TypeWidget other) {
                PromiseAdapter promiseAdapter = other.getPromiseAdapter();
                return new ListenableFutureResultType(typeAdapter.unifyTypes(valueType, promiseAdapter.getResultType()));
            }
        };
    }

}
