/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Opcodes;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FuturePromiseAdapter implements PromiseAdapter {
    private TypeWidget valueType;

    public FuturePromiseAdapter(TypeWidget valueType) {
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getResultType() {
        return valueType;
    }

    @Override
    public BytecodeExpression resolve(ScopedBuilder scope, BytecodeExpression timeout, BytecodeExpression target) {
        //     long getRemaining(TimeUnit units);
        BytecodeExpression units = scope.constant(TimeUnit.NANOSECONDS);
        return new BytecodeCastExpression(valueType,
                ExactInvocation.boundInvoke(Opcodes.INVOKEINTERFACE, "get", scope.adapt(Future.class, false), AnyTypeWidget.getInstance(),
                        target,
                        ExactInvocation.boundInvoke(Opcodes.INVOKEINTERFACE, "getRemaining", BaseTypeAdapter.TIMEOUT_CORE, BaseTypeAdapter.INT64, timeout, units).invoke(Location.NONE),
                        units).invoke(Location.NONE));
    }
}
