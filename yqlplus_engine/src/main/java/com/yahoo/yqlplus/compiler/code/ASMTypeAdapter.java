/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.TypeLiteral;
import com.yahoo.yqlplus.api.types.YQLArrayType;
import com.yahoo.yqlplus.api.types.YQLBaseType;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.api.types.YQLMapType;
import com.yahoo.yqlplus.api.types.YQLOptionalType;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.compiler.runtime.Result;
import com.yahoo.yqlplus.compiler.runtime.YQLError;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.WildcardType;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ASMTypeAdapter implements EngineValueTypeAdapter {
    private final ASMClassSource source;
    private final BaseTypeAdapter baseTypeAdapter;
    private final Map<YQLType, TypeWidget> resolved = Maps.newLinkedHashMap();

    private final Map<TypeLiteral<?>, TypeWidget> resolvedInternal = Maps.newLinkedHashMap();
    private final Iterable<TypeAdaptingWidget> adapterChain;
    private final Map<TypeWidget, TypeWidget> resultTypes = Maps.newLinkedHashMap();

    public ASMTypeAdapter(ASMClassSource source, Set<TypeAdaptingWidget> adapters, TypeAdaptingWidget defaultAdaptingWidget) {
        this.source = source;
        this.baseTypeAdapter = new BaseTypeAdapter();
        this.adapterChain = Iterables.concat(ImmutableList.of(new ArrayTypeAdapter()), adapters, ImmutableList.of(defaultAdaptingWidget));
    }

    private ASMTypeAdapter(ASMClassSource source, BaseTypeAdapter baseTypeAdapter, Iterable<TypeAdaptingWidget> adapterChain) {
        this.source = source;
        this.baseTypeAdapter = baseTypeAdapter;
        this.adapterChain = adapterChain;
    }

    public ASMTypeAdapter createChild(ASMClassSource parentSource) {
        return new ASMTypeAdapter(parentSource, baseTypeAdapter, adapterChain);
    }


    @Override
    public BytecodeExpression constant(TypeWidget type, Object value) {
        return source.constant(type, value);
    }

    @Override
    public BytecodeExpression constant(Object value) {
        return source.constant(inferConstantType(value), value);
    }


    public YQLType createYQLType(TypeWidget value) {
        YQLType output = adaptTypeWidgetValue(value);
        if (value.isNullable()) {
            return YQLOptionalType.create(output);
        }
        return output;
    }

    private YQLType adaptTypeWidgetValue(TypeWidget value) {
        switch (value.getValueCoreType()) {
            case VOID:
                return YQLBaseType.VOID;
            case INT8:
                return YQLBaseType.INT8;
            case INT16:
                return YQLBaseType.INT16;
            case INT32:
                return YQLBaseType.INT32;
            case INT64:
                return YQLBaseType.INT64;
            case FLOAT32:
                return YQLBaseType.FLOAT32;
            case FLOAT64:
                return YQLBaseType.FLOAT64;
            case STRING:
                return YQLBaseType.STRING;
            case BYTES:
                return YQLBaseType.BYTES;
            case TIMESTAMP:
                return YQLBaseType.TIMESTAMP;
            case BOOLEAN:
                return YQLBaseType.BOOLEAN;
            case MAP: {
                if (value.isIndexable()) {
                    IndexAdapter indexAdapter = value.getIndexAdapter();
                    YQLType keyType = adaptTypeWidgetValue(indexAdapter.getKey());
                    YQLType valueType = adaptTypeWidgetValue(indexAdapter.getValue());
                    return YQLMapType.create(keyType, valueType);
                } else {
                    throw new ProgramCompileException("Unsupported core type MAP for non-Indexable type %s", value);
                }
            }
            case ARRAY: {
                if (value.isIndexable() && value.isIterable()) {
                    IndexAdapter indexAdapter = value.getIndexAdapter();
                    YQLType valueType = adaptTypeWidgetValue(indexAdapter.getValue());
                    return YQLArrayType.create(valueType);
                } else if (value.isIterable()) {
                    IterateAdapter iterableAdapter = value.getIterableAdapter();
                    YQLType valueType = adaptTypeWidgetValue(iterableAdapter.getValue());
                    return YQLArrayType.create(valueType);
                } else {
                    throw new ProgramCompileException("Unsupported core type ARRAY for non-Indexable/Iterable type %s", value);
                }
            }
            case STRUCT: {
                if (value.hasProperties()) {
                    PropertyAdapter propertyAdapter = value.getPropertyAdapter();
                    YQLStructType.Builder structBuilder = YQLStructType.builder();
                    structBuilder.setClosed(propertyAdapter.isClosed());
                    if (propertyAdapter.isClosed()) {
                        for (PropertyAdapter.Property property : propertyAdapter.getProperties()) {
                            YQLType vtr = adaptTypeWidgetValue(property.type);
                            structBuilder.addField(property.name, vtr);
                        }
                    }
                    return structBuilder.build();
                } else {
                    throw new ProgramCompileException("Unsupported core type STRUCT for non-Property value type %s", value);

                }
            }
            case ANY:
                return YQLBaseType.ANY;
            case UNION:
            case OPTIONAL:
            default:
                throw new UnsupportedOperationException("Unsupported CoreType for TypeWidget: " + value.getValueCoreType());
        }
    }

    private TypeWidget selectMax(TypeWidget left, TypeWidget right) {
        if (left.getValueCoreType().ordinal() > right.getValueCoreType().ordinal()) {
            return left;
        } else {
            return right;
        }
    }

    public boolean isNumeric(TypeWidget type) {
        return isInteger(type) || isFloat(type);
    }

    public boolean isInteger(TypeWidget type) {
        return YQLBaseType.INTEGERS.contains(type.getValueCoreType());
    }

    public boolean isFloat(TypeWidget type) {
        return YQLBaseType.FLOATS.contains(type.getValueCoreType());
    }

    @Override
    public TypeWidget unifyTypes(TypeWidget leftType, TypeWidget rightType) {
        if(leftType == rightType) {
            return leftType;
        }
        boolean nullable = leftType.isNullable() || rightType.isNullable();
        boolean primitive = leftType.isPrimitive() && rightType.isPrimitive();
        if (!primitive) {
            leftType = leftType.boxed();
            rightType = rightType.boxed();
        }
        TypeWidget result;
        if (leftType.getValueCoreType() == YQLCoreType.ANY || rightType.getValueCoreType() == YQLCoreType.ANY) {
            result = AnyTypeWidget.getInstance();
        } else if ((isInteger(leftType) && isInteger(rightType))
                || (isFloat(leftType) && isFloat(rightType))) {
            result = selectMax(leftType, rightType);
        } else if (isNumeric(leftType) && isFloat(rightType)) {
            // fine, just coerce them both to FLOAT64
            result = BaseTypeAdapter.FLOAT64;
        } else if (isNumeric(rightType) && isFloat(leftType)) {
            // fine, just coerce them both to FLOAT64
            result = BaseTypeAdapter.FLOAT64;
        } else if (leftType.getValueCoreType() == YQLCoreType.BOOLEAN && rightType.getValueCoreType() == YQLCoreType.BOOLEAN) {
            // both boolean!
            result = BaseTypeAdapter.BOOLEAN;
        } else if (leftType.getJVMType().getDescriptor().equals(rightType.getJVMType().getDescriptor())) {
            switch (leftType.getValueCoreType()) {
                case VOID:
                case BOOLEAN:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case TIMESTAMP:
                case FLOAT32:
                case FLOAT64:
                case OBJECT:
                case OPTIONAL:
                case STRING:
                case ENUM:
                case ANY:
                case NAME_PAIR:
                case ERROR:
                case BYTES: {
                    result = leftType;
                    break;
                }
                case MAP:
                case ARRAY:
                case STRUCT:
                case SEQUENCE:
                case PROMISE:
                case RESULT:
                    if (leftType.hasUnificationAdapter()) {
                        result = leftType.getUnificationAdapter(this).unify(rightType);
                    } else if (rightType.hasUnificationAdapter()) {
                        result = rightType.getUnificationAdapter(this).unify(leftType);
                    } else {
                        result = AnyTypeWidget.getInstance();
                    }
                    break;
                default:
                    result = AnyTypeWidget.getInstance();
            }
        } else {
            result = AnyTypeWidget.getInstance();
        }
        if (nullable) {
            return NullableTypeWidget.create(result.boxed());
        } else {
            return NotNullableTypeWidget.create(result.unboxed());
        }
    }

    @Override
    public TypeWidget unifyTypes(Iterable<TypeWidget> types) {
        Iterator<TypeWidget> widget = types.iterator();
        if (!widget.hasNext()) {
            throw new ProgramCompileException("Empty Type list (should not happen)");
        }
        TypeWidget current = widget.next();
        while (widget.hasNext()) {
            TypeWidget next = widget.next();
            current = unifyTypes(current, next);
        }
        return current;
    }


    @Override
    public TypeWidget inferConstantType(Object value) {
        if (value == null) {
            return AnyTypeWidget.getInstance();
        }
        Class<?> clazz = value.getClass();
        TypeWidget adapted = baseTypeAdapter.adaptInternal(clazz);
        if (adapted != null) {
            if (adapted.unboxed() != adapted) {
                return adapted.unboxed();
            }
            return adapted;
        }
        if (List.class.isInstance(value)) {
            List<?> lst = (List) value;
            if (lst.isEmpty()) {
                return new ListTypeWidget(BaseTypeAdapter.ANY);
            }
            List<TypeWidget> types = Lists.newArrayList();
            for (Object item : lst) {
                types.add(inferConstantType(item));
            }
            TypeWidget output = unifyTypes(types);
            return new ListTypeWidget(output);
        } else if (Map.class.isInstance(value)) {
            Map<?, ?> map = (Map) value;
            if (map.isEmpty()) {
                return new MapTypeWidget(BaseTypeAdapter.ANY, BaseTypeAdapter.ANY);
            }
            List<TypeWidget> keyTypes = Lists.newArrayList();
            List<TypeWidget> valueTypes = Lists.newArrayList();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                keyTypes.add(inferConstantType(entry.getKey()));
                valueTypes.add(inferConstantType(entry.getValue()));
            }
            return new MapTypeWidget(unifyTypes(keyTypes), unifyTypes(valueTypes));
        }
        return NotNullableTypeWidget.create(adaptInternal(clazz));
    }

    @Override
    public TypeWidget adaptInternal(TypeLiteral<?> typeLiteral) {
        TypeWidget out = baseTypeAdapter.adaptInternal(typeLiteral);
        if (out != null) {
            return out;
        }
        if (resolvedInternal.containsKey(typeLiteral)) {
            return resolvedInternal.get(typeLiteral);
        }
        for (TypeAdaptingWidget adapterWidget : adapterChain) {
            if (adapterWidget.supports(typeLiteral.getRawType())) {
                TypeWidget result = adapterWidget.adapt(this, typeLiteral.getType());
                if (result != null) {
                    resolvedInternal.put(typeLiteral, result);
                    return result;
                }
            }
        }
        throw new UnsupportedOperationException("Unable to adapt " + typeLiteral);
    }

    @Override
    public TypeWidget adaptInternal(java.lang.reflect.Type type) {
        return adaptInternal(TypeLiteral.get((type instanceof WildcardType)? type.getClass():type));
    }

    @Override
    public TypeWidget adaptInternal(Class<?> clazz) {
        return adaptInternal(TypeLiteral.get(clazz));
    }

    @Override
    public TypeWidget adapt(YQLType type) {
        return defaultRepresentation(type);
    }

    public TypeWidget defaultRepresentation(YQLType valueType) {
        TypeWidget out = baseTypeAdapter.adapt(valueType);
        if (out != null) {
            return out;
        }
        out = resolved.get(valueType);
        if (out != null) {
            return out;
        }
        out = createYQLTypeWidget(valueType);
        resolved.put(valueType, out);
        return out;
    }

    private TypeWidget createYQLTypeWidget(YQLType valueType) {
        switch (valueType.getCoreType()) {
            case MAP:
                return createMapType((YQLMapType) valueType);
            case ARRAY:
                return createArrayType((YQLArrayType) valueType);
            case UNION:
            case ANY:
                return AnyTypeWidget.getInstance();
            case STRUCT:
                return createStruct((YQLStructType) valueType);
            case OPTIONAL: {
                YQLOptionalType optional = (YQLOptionalType) valueType;
                return NullableTypeWidget.create(defaultRepresentation(optional.getValueType()).boxed());
            }
            default:
                // all the basic type should have been resolved by baseTypeAdapter
                throw new ProgramCompileException("Unknown/unexpected core type: %s", valueType);
        }
    }

    private TypeWidget createMapType(YQLMapType valueType) {
        TypeWidget key = defaultRepresentation(valueType.getKeyType());
        TypeWidget value = defaultRepresentation(valueType.getValueType());
        return new MapTypeWidget(key, value);
    }

    private TypeWidget createArrayType(YQLArrayType valueType) {
        TypeWidget value = defaultRepresentation(valueType.getValueType());
        return new ListTypeWidget(value);
    }

    private int structGen = 0;

    private TypeWidget createStruct(YQLStructType valueType) {
        if (!valueType.isClosed()) {
            return BaseTypeAdapter.STRUCT;
        }
        StructGenerator struct = new StructGenerator("struct_" + (++structGen), source);
        struct.define(valueType);
        return struct.getType();
    }


    public Type getType(Class<?> rawClass) {
        return Type.getType(rawClass);
    }

    @Override
    public TypeWidget adaptInternal(TypeLiteral<?> typeLiteral, boolean nullable) {
        TypeWidget output = adaptInternal(typeLiteral);
        return nullable ? NullableTypeWidget.create(output) : NotNullableTypeWidget.create(output);
    }

    @Override
    public TypeWidget adaptInternal(Class<?> clazz, boolean nullable) {
        TypeWidget output = adaptInternal(clazz);
        return nullable ? NullableTypeWidget.create(output) : NotNullableTypeWidget.create(output);
    }

    public TypeWidget resultTypeFor(TypeWidget valueType) {
        if (resultTypes.containsKey(valueType)) {
            return resultTypes.get(valueType);
        }
        TypeWidget rt = createResultType(valueType);
        resultTypes.put(valueType, rt);
        return rt;
    }

    private TypeWidget createResultType(final TypeWidget valueType) {
        return new BaseTypeWidget(Type.getType(Result.class)) {
            @Override
            public YQLCoreType getValueCoreType() {
                return YQLCoreType.RESULT;
            }

            @Override
            public boolean isResult() {
                return true;
            }

            @Override
            public ResultAdapter getResultAdapter() {
                return new ResultResultAdapter(this, valueType);
            }

        };
    }

    private class ResultResultAdapter implements ResultAdapter {
        private final TypeWidget ownerType;
        private final TypeWidget valueType;

        public ResultResultAdapter(TypeWidget ownerType, TypeWidget valueType) {
            this.ownerType = ownerType;
            this.valueType = valueType;
        }

        @Override
        public TypeWidget getResultType() {
            return valueType;
        }

        @Override
        public BytecodeExpression createSuccess(BytecodeExpression input) {
            return ownerType.construct(new BytecodeCastExpression(AnyTypeWidget.getInstance(), input));
        }

        @Override
        public BytecodeExpression createFailureThrowable(BytecodeExpression input) {
            return ownerType.construct(new BytecodeCastExpression(adaptInternal(Throwable.class), input));
        }

        @Override
        public BytecodeExpression createFailureYQLError(BytecodeExpression input) {
            return ownerType.construct(new BytecodeCastExpression(adaptInternal(YQLError.class), input));
        }

        @Override
        public BytecodeExpression resolve(BytecodeExpression target) {
            GambitCreator.Invocable invocable = ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "resolve", ownerType, AnyTypeWidget.getInstance(), target);
            return new BytecodeCastExpression(valueType, invocable.invoke(Location.NONE, ImmutableList.of()));
        }

        @Override
        public BytecodeExpression isSuccess(final BytecodeExpression target) {
            return new BaseTypeExpression(BaseTypeAdapter.BOOLEAN) {
                @Override
                public void generate(CodeEmitter code) {
                    Label isFalse = new Label();
                    target.generate(code);
                    code.cast(ownerType, target.getType(), isFalse);
                    MethodVisitor mv = code.getMethodVisitor();
                    mv.visitFieldInsn(Opcodes.GETFIELD, ownerType.getJVMType().getInternalName(), "failure", Type.getInternalName(YQLError.class));
                    mv.visitJumpInsn(Opcodes.IFNONNULL, isFalse);
                    code.emitBooleanConstant(true);
                    Label done = new Label();
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                    mv.visitLabel(isFalse);
                    code.emitBooleanConstant(false);
                    mv.visitLabel(done);
                }
            };
        }
    }
}
