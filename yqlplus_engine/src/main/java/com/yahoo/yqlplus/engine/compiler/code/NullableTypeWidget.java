/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import org.objectweb.asm.Type;

import java.util.List;

public class NullableTypeWidget implements TypeWidget {
    public static TypeWidget create(TypeWidget input) {
        if (input.isPrimitive()) {
            return input;
        } else if (!input.isNullable()) {
            if (input instanceof NotNullableTypeWidget) {
                return ((NotNullableTypeWidget) input).getTarget();
            }
            return new NullableTypeWidget(input);
        } else {
            return input;
        }
    }

    private final TypeWidget target;

    public TypeWidget getTarget() {
        return target;
    }

    private NullableTypeWidget(TypeWidget target) {
        this.target = target;
    }

    @Override
    public Type getJVMType() {
        return target.getJVMType();
    }

    @Override
    public boolean isPrimitive() {
        return target.isPrimitive();
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        return target.construct(arguments);
    }

    @Override
    public Coercion coerceTo(BytecodeExpression source, TypeWidget target) {
        return this.target.coerceTo(source, target);
    }

    @Override
    public TypeWidget boxed() {
        return target.boxed();
    }

    @Override
    public TypeWidget unboxed() {
        return target.unboxed();
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments) {
        return this.target.invoke(target, outputType, methodName, arguments);
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return target.getPropertyAdapter();
    }

    @Override
    public boolean hasProperties() {
        return target.hasProperties();
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return target.getIndexAdapter();
    }

    @Override
    public boolean isIndexable() {
        return target.isIndexable();
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return target.getIterableAdapter();
    }

    @Override
    public boolean isIterable() {
        return target.isIterable();
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return target.getValueCoreType();
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return new NullTestingComparisonAdapter(this, target);
    }

    @Override
    public PromiseAdapter getPromiseAdapter() {
        return target.getPromiseAdapter();
    }

    @Override
    public boolean isPromise() {
        return target.isPromise();
    }

    @Override
    public String getTypeName() {
        return target.getTypeName();
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        return target.isAssignableFrom(type);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return target.hasUnificationAdapter();
    }

    @Override
    public UnificationAdapter getUnificationAdapter(EngineValueTypeAdapter typeAdapter) {
        return target.getUnificationAdapter(typeAdapter);
    }

    @Override
    public String toString() {
        return "NullableType<" +
                target +
                '>';
    }
}
