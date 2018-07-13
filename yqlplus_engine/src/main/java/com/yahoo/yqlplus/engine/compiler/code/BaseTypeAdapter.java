/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableMap;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.types.YQLArrayType;
import com.yahoo.yqlplus.api.types.YQLBaseType;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.api.types.YQLMapType;
import com.yahoo.yqlplus.api.types.YQLOptionalType;
import com.yahoo.yqlplus.api.types.YQLType;
import org.objectweb.asm.Type;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

public class BaseTypeAdapter implements ValueTypeAdapter {
    public static final TypeWidget VOID = new CoreTypeWidget(Type.VOID_TYPE, YQLCoreType.VOID) {
    };

    public static final TypeWidget INT8;
    public static final TypeWidget INT16;
    public static final TypeWidget INT32;
    public static final TypeWidget INT64;
    public static final TypeWidget FLOAT32;
    public static final TypeWidget FLOAT64;
    public static final TypeWidget BOOLEAN;
    public static final TypeWidget STRING;
    public static final TypeWidget BYTES;
    public static final TypeWidget BYTEBUFFER;
    public static final TypeWidget TIMESTAMP;
    public static final TypeWidget STRUCT = new DynamicRecordWidget();
    public static final TypeWidget ANY = AnyTypeWidget.getInstance();

    public static final TypeWidget TIMEOUT_CORE = new CoreTypeWidget(Type.getType(Timeout.class), YQLCoreType.OBJECT) {
    };

    private static final ImmutableMap<String, TypeWidget> STATIC_MAPPINGS;
    private static final ImmutableMap<YQLType, TypeWidget> YQL_MAPPINGS;

    static {
        ImmutableMap.Builder<String, TypeWidget> m = ImmutableMap.builder();
        ImmutableMap.Builder<YQLType, TypeWidget> t = ImmutableMap.builder();
        INT8 = registerPair(YQLBaseType.INT8, m, t, Type.BYTE_TYPE, Type.getType(Byte.class));
        INT16 = registerPair(YQLBaseType.INT16, m, t, Type.SHORT_TYPE, Type.getType(Short.class));
        INT32 = registerPair(YQLBaseType.INT32, m, t, Type.INT_TYPE, Type.getType(Integer.class));
        INT64 = registerPair(YQLBaseType.INT64, m, t, Type.LONG_TYPE, Type.getType(Long.class));
        FLOAT32 = registerPair(YQLBaseType.FLOAT32, m, t, Type.FLOAT_TYPE, Type.getType(Float.class));
        FLOAT64 = registerPair(YQLBaseType.FLOAT64, m, t, Type.DOUBLE_TYPE, Type.getType(Double.class));
        BOOLEAN = registerPair(YQLBaseType.BOOLEAN, m, t, Type.BOOLEAN_TYPE, Type.getType(Boolean.class));
        registerJavaPair(YQLBaseType.STRING, m, Type.CHAR_TYPE, Type.getType(Character.class));

        STRING = register(YQLBaseType.STRING, m, t, new StringTypeWidget());
        BYTEBUFFER = register(YQLBaseType.BYTES, m, t, new ByteBufferTypeWidget());
        BYTES = registerJavaType(YQLBaseType.BYTES, m, new ByteArrayTypeWidget());
        registerJavaType(YQLBaseType.TIMESTAMP, m, new DateTypeWidget());
        registerJavaType(YQLBaseType.TIMESTAMP, m, new SqlDateTypeWidget());

        // TODO: JDK 1.8 time types
        // TODO: JodaTime adapter
        m.put(ANY.getJVMType().getDescriptor(), ANY);
        m.put(VOID.getJVMType().getDescriptor(), VOID);
        STATIC_MAPPINGS = m.build();
        TIMESTAMP = registerYQL(YQLBaseType.TIMESTAMP, t, INT64, INT64.boxed());

        registerArrayType(t, YQLBaseType.STRING, STRING);
        registerArrayType(t, YQLBaseType.ANY, ANY);
        registerMapType(t, YQLBaseType.ANY, YQLBaseType.ANY, ANY, ANY);
        registerMapType(t, YQLBaseType.STRING, YQLBaseType.ANY, STRING, ANY);
        registerMapType(t, YQLBaseType.STRING, YQLBaseType.STRING, STRING, STRING);
        registerMapType(t, YQLBaseType.INT32, YQLBaseType.ANY, INT32, ANY);

        YQL_MAPPINGS = t.build();
    }

    private abstract static class CoreTypeWidget extends BaseTypeWidget {
        private YQLCoreType coreType;

        private CoreTypeWidget(Type type, YQLCoreType coreType) {
            super(type);
            this.coreType = coreType;
        }

        @Override
        public YQLCoreType getValueCoreType() {
            return coreType;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public String toString() {
            return coreType.toString();
        }
    }

    private static final class StringTypeWidget extends CoreTypeWidget {
        public StringTypeWidget() {
            super(Type.getType(String.class), YQLCoreType.STRING);
        }

    }

    private static final class ByteBufferTypeWidget extends CoreTypeWidget {
        public ByteBufferTypeWidget() {
            super(Type.getType(ByteBuffer.class), YQLCoreType.BYTES);
        }

    }

    private static final class ByteArrayTypeWidget extends CoreTypeWidget {
        public ByteArrayTypeWidget() {
            super(Type.getType(byte[].class), YQLCoreType.BYTES);
        }

    }

    private static final class DateTypeWidget extends CoreTypeWidget {
        public DateTypeWidget() {
            super(Type.getType(Date.class), YQLCoreType.TIMESTAMP);
        }

    }

    private static final class SqlDateTypeWidget extends CoreTypeWidget {
        public SqlDateTypeWidget() {
            super(Type.getType(java.sql.Date.class), YQLCoreType.TIMESTAMP);
        }

    }

    private static TypeWidget registerJavaType(YQLType type, ImmutableMap.Builder<String, TypeWidget> m, TypeWidget btw) {
        m.put(btw.getJVMType().getDescriptor(), NullableTypeWidget.create(btw));
        return btw;
    }

    private static TypeWidget register(YQLType type, ImmutableMap.Builder<String, TypeWidget> m, ImmutableMap.Builder<YQLType, TypeWidget> t, TypeWidget coreTypeWidget) {
        t.put(type, NotNullableTypeWidget.create(coreTypeWidget));
        final YQLType optionalType = YQLOptionalType.create(type);
        final TypeWidget nullableType = NullableTypeWidget.create(coreTypeWidget);
        t.put(optionalType, nullableType);
        m.put(coreTypeWidget.getJVMType().getDescriptor(), nullableType);
        return nullableType;
    }

    private static TypeWidget registerYQL(YQLType type, ImmutableMap.Builder<YQLType, TypeWidget> t, TypeWidget main, TypeWidget optional) {
        t.put(type, main);
        t.put(YQLOptionalType.create(type), optional);
        return optional;
    }

    private static TypeWidget registerPair(YQLType type, ImmutableMap.Builder<String, TypeWidget> m, ImmutableMap.Builder<YQLType, TypeWidget> t, Type primitiveType, Type boxedType) {
        PrimitiveTypeWidget primitive = new PrimitiveTypeWidget(primitiveType, type.getCoreType());
        BoxedTypeWidget boxed = new BoxedTypeWidget(type.getCoreType(), boxedType, primitive);
        primitive.boxed = boxed;
        TypeWidget boxedValue = NotNullableTypeWidget.create(boxed);
        TypeWidget optionalBoxedValue = NullableTypeWidget.create(boxed);
        m.put(primitiveType.getDescriptor(), primitive);
        m.put(boxedType.getDescriptor(), optionalBoxedValue);
        t.put(type, primitive);
        t.put(YQLOptionalType.create(type), optionalBoxedValue);
        registerArrayType(t, type, boxedValue);
        registerArrayType(t, YQLOptionalType.create(type), optionalBoxedValue);
        return primitive;
    }

    private static TypeWidget registerJavaPair(YQLType type, ImmutableMap.Builder<String, TypeWidget> m, Type primitiveType, Type boxedType) {
        PrimitiveTypeWidget primitive = new PrimitiveTypeWidget(primitiveType, type.getCoreType());
        BoxedTypeWidget boxed = new BoxedTypeWidget(type.getCoreType(), boxedType, primitive);
        primitive.boxed = boxed;
        TypeWidget optionalBoxedValue = NullableTypeWidget.create(boxed);
        m.put(primitiveType.getDescriptor(), primitive);
        m.put(boxedType.getDescriptor(), optionalBoxedValue);
        return primitive;
    }

    private static void registerArrayType(ImmutableMap.Builder<YQLType, TypeWidget> t, YQLType yqlType, TypeWidget type) {
        ListTypeWidget widget = new ListTypeWidget(type);
        YQLType arrayOf = YQLArrayType.create(yqlType);
        YQLType optionalArrayof = YQLOptionalType.create(arrayOf);
        t.put(arrayOf, widget);
        t.put(optionalArrayof, widget);
    }

    private static void registerMapType(ImmutableMap.Builder<YQLType, TypeWidget> t, YQLType keyType, YQLType valueType, TypeWidget key, TypeWidget value) {
        MapTypeWidget widget = new MapTypeWidget(Type.getType(Map.class), key, value);
        YQLType mapOf = YQLMapType.create(keyType, valueType);
        YQLType optionalArrayof = YQLOptionalType.create(mapOf);
        t.put(mapOf, widget);
        t.put(optionalArrayof, widget);
    }

    public TypeWidget adapt(YQLType type) {
        return YQL_MAPPINGS.get(type);
    }

    @Override
    public TypeWidget adaptInternal(TypeLiteral<?> typeLiteral) {
        return adaptInternal(typeLiteral.getRawType());
    }

    @Override
    public TypeWidget adaptInternal(java.lang.reflect.Type type, boolean nullable) {
        return adaptInternal(TypeLiteral.get(type).getRawType(), nullable);
    }

    @Override
    public TypeWidget adaptInternal(java.lang.reflect.Type type) {
        return adaptInternal(TypeLiteral.get(type).getRawType());
    }

    @Override
    public TypeWidget adaptInternal(Class<?> clazz) {
        return STATIC_MAPPINGS.get(Type.getDescriptor(clazz));
    }

    @Override
    public TypeWidget adaptInternal(Class<?> clazz, boolean nullable) {
        TypeWidget output = adaptInternal(clazz);
        return nullable ? NullableTypeWidget.create(output) : NotNullableTypeWidget.create(output);
    }
}
