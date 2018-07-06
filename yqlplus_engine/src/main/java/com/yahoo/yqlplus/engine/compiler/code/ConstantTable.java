/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Type;

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

import static org.objectweb.asm.Opcodes.*;

public class ConstantTable {
    private static final Set<String> CONSTANT_TYPES = ImmutableSet.of(Type.getDescriptor(String.class),
            Type.getDescriptor(float.class),
            Type.getDescriptor(double.class),
            Type.getDescriptor(int.class),
            Type.getDescriptor(long.class));
    private static final Set<Class<?>> BOXED_CONSTANTS = ImmutableSet.of(Float.class, Double.class, Integer.class, Long.class);


    private final ASMClassSource source;
    private final Map<String, Object> constantTable = Maps.newLinkedHashMap();
    private final Map<ConstantKey, ConstantValue> constantValues = Maps.newHashMap();
    private final ConstantTableClass table;

    public ConstantTable(ASMClassSource source) {
        this.source = source;
        this.table = new ConstantTableClass(source);

    }

    private class ConstantTableClass extends UnitGenerator {
        private ConstantTableClass(ASMClassSource environment) {
            super("CONSTANTS", environment);
        }

        @Override
        public void prepare(Class<?> clazz) {
            super.prepare(clazz);
            for (Map.Entry<String, Object> e : constantTable.entrySet()) {
                try {
                    clazz.getField(e.getKey()).set(null, e.getValue());
                } catch (IllegalAccessException | NoSuchFieldException e1) {
                    throw new ProgramCompileException(e1);
                }
            }
        }

        @Override
        public void generate(ClassSink cws) {
            super.generate(cws);
        }
    }

    private class ConstantValue extends BaseTypeExpression implements EvaluatedExpression {
        private String name;

        private ConstantValue(TypeWidget type, String name) {
            super(type);
            this.name = name;
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitFieldInsn(GETSTATIC, table.getInternalName(), name, getType().getJVMType().getDescriptor());
        }
    }

    private final class ConstantKey {
        final TypeWidget type;
        final Object value;

        private ConstantKey(TypeWidget type, Object value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConstantKey that = (ConstantKey) o;

            if (!type.equals(that.type)) return false;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }
    }

    public BytecodeExpression constant(TypeWidget type, Object value) {
        // Integer, a Float, a Long, a Double, a String
        if (CONSTANT_TYPES.contains(type.getJVMType().getDescriptor())) {
            return new LDCConstant(NotNullableTypeWidget.create(type), value);
        } else if (!type.isPrimitive() && BOXED_CONSTANTS.contains(value.getClass())) {
            return new BoxedLDCConstant(NotNullableTypeWidget.create(type), value);
        } else if (value.getClass() == Class.class) {
            source.addClass((Class<?>) value);
            return new LDCConstant(type, Type.getType((Class) value));
        } else if (value.getClass() == Boolean.class) {
            if (type.isPrimitive()) {
                return new InstructionConstant(type, ((Boolean) value) ? ICONST_1 : ICONST_0);
            } else {
                return new BoxedLDCConstant(NotNullableTypeWidget.create(source.adaptInternal(Boolean.class)), ((Boolean) value) ? 1 : 0);
            }
        } else if (Enum.class.isAssignableFrom(value.getClass())) {
            return new EnumConstant(NotNullableTypeWidget.create(source.adaptInternal(value.getClass())), (Enum) value);
        }
        ConstantKey key = new ConstantKey(type, value);
        if (constantValues.containsKey(key)) {
            return constantValues.get(key);
        }


        // Class -> Type -> constant
        // boolean true / false and null we can use instructions for
        // anything else we inject as a field + constructor argument
        String name = "constant_" + constantTable.size();
        ConstantValue val = new ConstantValue(NotNullableTypeWidget.create(type), name);
        FieldDefinition defn = table.createField(type, name);
        defn.setModifiers(Modifier.STATIC | Modifier.PUBLIC);
        constantTable.put(name, value);
        constantValues.put(key, val);
        return val;
    }
}
