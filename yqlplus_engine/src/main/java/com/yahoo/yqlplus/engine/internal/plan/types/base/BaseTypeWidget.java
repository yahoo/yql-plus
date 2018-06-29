/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.api.types.YQLBaseType;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ConstructInvocation;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ResultAdapter;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.ConstructorGenerator;
import com.yahoo.yqlplus.engine.internal.plan.types.*;
import com.yahoo.yqlplus.language.parser.Location;

import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

public abstract class BaseTypeWidget implements TypeWidget {
    private final Type type;

    public BaseTypeWidget(Type type) {
        this.type = type;
    }

    @Override
    public abstract YQLCoreType getValueCoreType();

    @Override
    public Type getJVMType() {
        return type;
    }

    @Override
    public String getTypeName() {
        switch (type.getSort()) {
            case Type.ARRAY:
                return type.getDescriptor();
            case Type.OBJECT:
                return type.getInternalName().replace("/", ".");
            case Type.VOID:
                return "void";
            case Type.METHOD:
                return type.getDescriptor();
            case Type.BOOLEAN:
                return "boolean";
            case Type.SHORT:
                return "short";
            case Type.INT:
                return "int";
            case Type.CHAR:
                return "char";
            case Type.FLOAT:
                return "float";
            case Type.LONG:
                return "long";
            case Type.DOUBLE:
                return "double";
            default:
                throw new UnsupportedOperationException("Unknown JVM type: " + type);
        }
    }

    @Override
    public boolean isPrimitive() {
        switch (type.getSort()) {
            case Type.ARRAY:
            case Type.OBJECT:
            case Type.VOID:
            case Type.METHOD:
                return false;
            case Type.BOOLEAN:
            case Type.SHORT:
            case Type.INT:
            case Type.CHAR:
            case Type.FLOAT:
            case Type.LONG:
            case Type.DOUBLE:
                return true;
            default:
                throw new UnsupportedOperationException("Unknown JVM type: " + type);
        }
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        if (type.getValueCoreType() == YQLCoreType.ANY) {
            return true;
        }
        switch (getValueCoreType()) {
            case VOID:
                return false;
            case BOOLEAN:
            case TIMESTAMP:
            case STRING:
            case BYTES:
            case FLOAT64:
                return getValueCoreType() == type.getValueCoreType();
            case ANY:
                return true;
            case INT8:
            case INT16:
            case INT32:
            case INT64: {
                if (YQLBaseType.INTEGERS.contains(type.getValueCoreType())) {
                    return getValueCoreType().ordinal() <= type.getValueCoreType().ordinal();
                } else {
                    return YQLBaseType.FLOATS.contains(type.getValueCoreType()) && (getValueCoreType() != YQLCoreType.INT64 || type.getValueCoreType() == YQLCoreType.FLOAT64);
                }
            }
            case FLOAT32:
                return YQLBaseType.FLOATS.contains(type.getValueCoreType());
            case ARRAY:
                if (type instanceof ListTypeWidget) {
                    return true;
                } else {
                    return isAssignableFrom(getJVMType(), type.getJVMType());
                }
            case MAP:
                if (type instanceof StructBaseTypeWidget) {
                     return true;
                }
            case ENUM:
            case UNION:
            case STRUCT:
            case OBJECT:
                return isAssignableFrom(getJVMType(), type.getJVMType());
            default:
                return false;
        }

    }
    
    private boolean isAssignableFrom(Type type1, Type type2) {
      try {
          Class clazz1 = Class.forName(type1.getClassName());
          Class clazz2 = Class.forName(type2.getClassName());
          if (clazz1.isAssignableFrom(clazz2)) {
              return true;
          }
      } catch (ClassNotFoundException e) {
      }
      return type2.getDescriptor().equals(type1.getDescriptor());
    }

    @Override
    public boolean isNullable() {
        return !isPrimitive();
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
    public boolean hasProperties() {
        return false;
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Coercion coerceTo(BytecodeExpression source, TypeWidget target) {
        if (getJVMType().getDescriptor().equals(target.getJVMType().getDescriptor())) {
            return new Coercion(0, source);
        }
        return null;
    }


    protected BytecodeExpression invokeNew(final Type type, BytecodeExpression... arguments) {
        return ConstructInvocation.boundInvoke(type, this, arguments).invoke(Location.NONE);
    }
    
    protected BytecodeExpression invokeNew(final Type type, final List<ConstructorGenerator> constructorGenerators, BytecodeExpression... arguments) {
        return ConstructInvocation.boundInvoke(type, this, constructorGenerators, arguments).invoke(Location.NONE);
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        if (isPrimitive()) {
            throw new UnsupportedOperationException();
        }
        return invokeNew(type, arguments);
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
    public IndexAdapter getIndexAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isIndexable() {
        return false;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isIterable() {
        return false;
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        Preconditions.checkState(!isPrimitive(), "BaseTypeWidget should not be handling a primitive type");
        return new ComparisonAdapter() {
            @Override
            public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
                // null or true
                if (isNullable()) {
                    scope.getMethodVisitor().visitJumpInsn(Opcodes.IFNULL, isNull);
                } else {
                    scope.pop(BaseTypeWidget.this);
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
    public boolean hasUnificationAdapter() {
        return false;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(ProgramValueTypeAdapter typeAdapter) {
        throw new UnsupportedOperationException();
    }
}
