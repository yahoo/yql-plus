/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import org.dynalang.dynalink.support.TypeUtilities;
import org.objectweb.asm.Type;

import java.util.List;

public class ReflectiveJavaTypeWidget extends BaseTypeWidget {
    private final EngineValueTypeAdapter adapter;
    private final Type type;
    private final TypeLiteral<?> typeLiteral;
    private final Class<?> clazz;
    private final MethodDispatcher dispatcher;
    private PropertyAdapter propertyAdapter;

    public ReflectiveJavaTypeWidget(EngineValueTypeAdapter adapter, Class<?> clazz) {
        this(adapter, TypeLiteral.get(clazz));
    }

    public ReflectiveJavaTypeWidget(EngineValueTypeAdapter adapter, TypeLiteral<?> typeLiteral) {
        super(Type.getType(typeLiteral.getRawType()));
        this.clazz = typeLiteral.getRawType();
        Preconditions.checkArgument(!clazz.isPrimitive(), "ReflectiveTypeWidget used on primitive: %s", clazz.getName());
        Preconditions.checkArgument(!clazz.isArray(), "ReflectiveTypeWidget used on array: %s", clazz.getName());
        Preconditions.checkArgument(TypeUtilities.getPrimitiveType(clazz) == null, "ReflectiveTypeWidget used on boxed primitive: %s", clazz.getName());
        this.adapter = adapter;
        this.type = Type.getType(typeLiteral.getRawType());
        this.typeLiteral = typeLiteral;
        this.dispatcher = new MethodDispatcher(typeLiteral);
    }

    @Override
    public YQLCoreType getValueCoreType() {
        if (isIterable()) {
            return YQLCoreType.ARRAY;
        } else if (hasProperties()) {
            return YQLCoreType.STRUCT;
        } else {
            return YQLCoreType.OBJECT;
        }
    }

    @Override
    public boolean isPrimitive() {
        return clazz.isPrimitive();
    }

    @Override
    public boolean isNullable() {
        return !clazz.isPrimitive();
    }

    @Override
    public Type getJVMType() {
        return type;
    }

    @Override
    public boolean isIterable() {
        return Iterable.class.isAssignableFrom(clazz);
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        if (isIterable()) {
            try {
                TypeLiteral<?> iteratorType = typeLiteral.getReturnType(clazz.getMethod("iterator"));
                return new JavaIterableAdapter(adapter.adaptInternal(JVMTypes.getTypeArgument(iteratorType.getType(), 0)));
            } catch (NoSuchMethodException e) {
                throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean isIndexable() {
        return hasProperties();
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        if (hasProperties()) {
            return new StructIndexAdapter(this, getPropertyAdapter());
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments) {
        final Type[] argTypes = new Type[arguments.size()];
        for (int i = 0; i < arguments.size(); ++i) {
            argTypes[i] = arguments.get(i).getType().getJVMType();
        }
        // semi-gross that we always use invokedynamic here, fix later
        List<BytecodeExpression> fullArgs = Lists.newArrayListWithCapacity(arguments.size() + 1);
        fullArgs.add(target);
        fullArgs.addAll(arguments);
        return new InvokeDynamicExpression(Dynamic.H_BOOTSTRAP, "dyn:callMethod:" + methodName, outputType, fullArgs);
    }

    @Override
    public boolean hasProperties() {
        if (Iterable.class.isAssignableFrom(clazz)) {
            return false;
        }
        PropertyAdapter properties = resolveProperties();
        if (properties == null) {
            return false;
        }
        return !properties.isClosed() || !Iterables.isEmpty(properties.getProperties());
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return resolveProperties();
    }

    private PropertyAdapter resolveProperties() {
        if (propertyAdapter == null) {
            propertyAdapter = ReflectivePropertyAdapter.create(this, adapter, typeLiteral);
        }
        return propertyAdapter;
    }

}
