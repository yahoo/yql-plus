/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ResultAdapter;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.*;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ArrayIndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ComparisonAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.IteratingSerializing;
import com.yahoo.yqlplus.engine.internal.plan.types.base.PropertyAdapter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

public class ArrayTypeWidget implements TypeWidget {
    private final Type type;
    private final TypeWidget valueType;

    public ArrayTypeWidget(Class<?> arrayType, TypeWidget valueType) {
        this.type = Type.getType(arrayType);
        this.valueType = valueType;
    }

    public ArrayTypeWidget(Type type, TypeWidget valueType) {
        this.type = type;
        this.valueType = valueType;
    }


    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.ARRAY;
    }

    @Override
    public Type getJVMType() {
        return type;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        return null;
    }

    @Override
    public Coercion coerceTo(BytecodeExpression source, TypeWidget target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeWidget boxed() {
        return this;
    }

    @Override
    public TypeWidget unboxed() {
        return this;
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, String methodName, List<BytecodeExpression> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasProperties() {
        return false;
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return new ArrayIndexAdapter(this, valueType);
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new ArrayIndexAdapter(this, valueType);
    }

    @Override
    public boolean isIterable() {
        return true;
    }

    @Override
    public SerializationAdapter getSerializationAdapter(NativeEncoding encoding) {
        return new IteratingSerializing(getIterableAdapter(), encoding);
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return new ComparisonAdapter() {
            @Override
            public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
                // null or true
                if (isNullable()) {
                    scope.getMethodVisitor().visitJumpInsn(Opcodes.IFNULL, isNull);
                } else {
                    scope.pop(ArrayTypeWidget.this);
                }
                scope.getMethodVisitor().visitJumpInsn(Opcodes.GOTO, isTrue);
            }
        };
    }

    @Override
    public boolean isPromise() {
        return false;
    }

    @Override
    public PromiseAdapter getPromiseAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isResult() {
        return false;
    }

    @Override
    public ResultAdapter getResultAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTypeName() {
        return type.getDescriptor();
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        return type.getValueCoreType() == YQLCoreType.ANY || getJVMType().getDescriptor().equals(type.getJVMType().getDescriptor());
    }

    @Override
    public boolean hasUnificationAdapter() {
        return false;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(ProgramValueTypeAdapter typeAdapter) {
        throw new UnsupportedOperationException();
    }
}
