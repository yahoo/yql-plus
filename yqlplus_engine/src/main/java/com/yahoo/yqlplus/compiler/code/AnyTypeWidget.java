/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

public class AnyTypeWidget implements TypeWidget {
    private static final Type OBJECT_TYPE = Type.getType(Object.class);
    private static final AnyTypeWidget ANY = new AnyTypeWidget();

    public static TypeWidget getInstance() {
        return ANY;
    }

    private AnyTypeWidget() {

    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.ANY;
    }

    public Type getJVMType() {
        return OBJECT_TYPE;
    }

    @Override
    public boolean isPrimitive() {
        return false;
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
    public boolean isNullable() {
        return true;
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        throw new UnsupportedOperationException();
    }

    static BytecodeExpression invokeDynamic(final String operationName, final TypeWidget expectedReturnType, BytecodeExpression target, List<BytecodeExpression> arguments) {
        List<BytecodeExpression> invokeArguments = Lists.newArrayListWithExpectedSize(arguments.size() + 1);
        invokeArguments.add(target);
        invokeArguments.addAll(arguments);
        return new InvokeDynamicExpression(Dynamic.H_BOOTSTRAP, operationName, expectedReturnType, invokeArguments);
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, String methodName, List<BytecodeExpression> arguments) {
        return invokeDynamic("dyn:callMethod:" + methodName, this, target, arguments);
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments) {
        return invokeDynamic("dyn:callMethod:" + methodName, outputType, target, arguments);
    }

    @Override
    public Coercion coerceTo(BytecodeExpression source, TypeWidget target) {
        return null;
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return new DynamicPropertyAdapter();
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return new DynamicIndexAdapter();
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new DynamicIterateAdapter();
    }

    @Override
    public boolean isIterable() {
        return true;
    }

    @Override
    public boolean hasProperties() {
        return true;
    }

    @Override
    public String toString() {
        return "ANY";
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        Preconditions.checkState(!isPrimitive(), "BaseTypeWidget should not be handling a primitive type");
        return new ComparisonAdapter() {
            @Override
            public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
                // null or true
                scope.nullTest(AnyTypeWidget.getInstance(), isNull);
                Label popTrue = new Label();
                scope.emitInstanceOf(AnyTypeWidget.getInstance(), Boolean.class, popTrue);
                final MethodVisitor mv = scope.getMethodVisitor();
                mv.visitMethodInsn(Opcodes.GETSTATIC, Type.getInternalName(Boolean.class), "TRUE", Type.getMethodDescriptor(Type.getType(Boolean.class)), false);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(Boolean.class), "equals", Type.getMethodDescriptor(Type.BOOLEAN_TYPE, Type.getType(Object.class)), false);
                mv.visitJumpInsn(Opcodes.IFEQ, isFalse);
                mv.visitJumpInsn(Opcodes.GOTO, isTrue);
                mv.visitLabel(popTrue);
                scope.pop(AnyTypeWidget.getInstance());
                mv.visitJumpInsn(Opcodes.GOTO, isTrue);
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
        return "ANY";
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        return true;
    }

    @Override
    public boolean hasUnificationAdapter() {
        return true;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(EngineValueTypeAdapter typeAdapter) {
        return new UnificationAdapter() {
            @Override
            public TypeWidget unify(TypeWidget other) {
                return AnyTypeWidget.getInstance();
            }
        };
    }
}
