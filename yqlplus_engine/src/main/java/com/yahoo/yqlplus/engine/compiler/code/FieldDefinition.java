/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.base.Preconditions;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Modifier;

import static org.objectweb.asm.Opcodes.*;

public class FieldDefinition extends Annotatable implements ObjectBuilder.FieldBuilder {
    final String ownerInternalName;
    int modifiers = Opcodes.ACC_PUBLIC;
    final TypeWidget type;
    final String name;
    final BytecodeExpression defaultValue;

    FieldDefinition(UnitGenerator owner, TypeWidget type, String name) {
        super(owner.environment);
        this.ownerInternalName = owner.getInternalName();
        this.type = type;
        this.name = name;
        this.defaultValue = null;
    }

    FieldDefinition(UnitGenerator owner, String name, BytecodeExpression defaultValue) {
        super(owner.environment);
        this.ownerInternalName = owner.getInternalName();
        this.type = defaultValue.getType();
        this.name = name;
        this.defaultValue = defaultValue;
    }

    FieldDefinition(ASMClassSource environment, String ownerInternalName, TypeWidget type, String name) {
        super(environment);
        this.ownerInternalName = ownerInternalName;
        this.type = type;
        this.name = name;
        this.defaultValue = null;
    }

    public int getModifiers() {
        return modifiers;
    }

    @Override
    public void addModifiers(int modifiers) {
        this.modifiers |= modifiers;
    }

    @Override
    public void setModifiers(int modifiers) {
        this.modifiers = modifiers;
    }

    @Override
    public TypeWidget getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public BytecodeExpression getDefaultValue() {
        return defaultValue;
    }

    @Override
    public AssignableValue get(final BytecodeExpression obj) {
        return new AssignableValue() {
            @Override
            public TypeWidget getType() {
                return type;
            }

            @Override
            public void generate(CodeEmitter code) {
                if (Modifier.isStatic(modifiers)) {
                    Preconditions.checkState(obj == null);
                    MethodVisitor mv = code.getMethodVisitor();
                    mv.visitFieldInsn(GETSTATIC, ownerInternalName, name, type.getJVMType().getDescriptor());
                } else {
                    obj.generate(code);
                    MethodVisitor mv = code.getMethodVisitor();
                    mv.visitFieldInsn(GETFIELD, ownerInternalName, name, type.getJVMType().getDescriptor());
                }
            }

            @Override
            public BytecodeExpression read() {
                return this;
            }

            @Override
            public BytecodeSequence write(final BytecodeExpression value) {
                return new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        if (Modifier.isStatic(modifiers)) {
                            value.generate(code);
                            MethodVisitor mv = code.getMethodVisitor();
                            mv.visitFieldInsn(PUTSTATIC, ownerInternalName, name, type.getJVMType().getDescriptor());
                        } else {
                            obj.generate(code);
                            value.generate(code);
                            MethodVisitor mv = code.getMethodVisitor();
                            mv.visitFieldInsn(PUTFIELD, ownerInternalName, name, type.getJVMType().getDescriptor());
                        }
                    }
                };
            }

            @Override
            public BytecodeSequence write(final TypeWidget top) {
                return new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        if (Modifier.isStatic(modifiers)) {
                            MethodVisitor mv = code.getMethodVisitor();
                            mv.visitFieldInsn(PUTSTATIC, ownerInternalName, name, type.getJVMType().getDescriptor());
                        } else {
                            MethodVisitor mv = code.getMethodVisitor();
                            obj.generate(code);
                            code.swap(top, obj.getType());
                            mv.visitFieldInsn(PUTFIELD, ownerInternalName, name, type.getJVMType().getDescriptor());
                        }
                    }
                };
            }
        };
    }

    public void generate(ClassVisitor cw) {
        FieldVisitor fv = cw.visitField(modifiers, name, type.getJVMType().getDescriptor(), null, null);
        generateAnnotations(fv);
        fv.visitEnd();
    }

    public void generateInit(CodeEmitter code) {
        if (defaultValue != null) {
            Preconditions.checkState(!Modifier.isStatic(modifiers), "static fields may not have defaultValue initializers");
            get(code.getLocal("this").read()).write(defaultValue).generate(code);
        }
    }

    public void addModifier(int modifier) {
        this.modifiers |= modifier;
    }
}
