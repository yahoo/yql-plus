/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;

public class ConstructorAdapter extends ExpressionHandler implements ObjectBuilder.ConstructorBuilder {
    private ConstructorGenerator generator;
    private GambitUnit unit;

    public ConstructorAdapter(ASMClassSource source, GambitUnit unit, ConstructorGenerator generator) {
        super(source);
        this.generator = generator;
        this.unit = unit;
        body = generator;
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        return generator.addArgument(name, type).read();
    }

    @Override
    public void setModifiers(int modifiers) {
        generator.setModifier(modifiers);
    }

    @Override
    public void addModifiers(int modifiers) {
        generator.addModifier(modifiers);
    }

    @Override
    public ObjectBuilder.AnnotationBuilder annotate(Class<?> clazz) {
        return generator.annotate(clazz);
    }

    @Override
    public void annotate(Annotation annotation) {
        generator.annotate(annotation);
    }

    @Override
    public ObjectBuilder.AnnotationBuilder annotate(org.objectweb.asm.Type type) {
        return generator.annotate(type);
    }

    @Override
    public void invokeSpecial(Class<?> clazz, BytecodeExpression... arguments) {
        invokeSpecial(clazz, Arrays.asList(arguments));
    }

    @Override
    public void invokeSpecial(final Class<?> clazz, final List<BytecodeExpression> arguments) {
        final Type[] args = new Type[arguments.size()];
        for (int i = 0; i < arguments.size(); ++i) {
            args[i] = arguments.get(i).getType().getJVMType();
        }
        unit.setSuperInit(
                new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.exec(code.getLocal("this"));
                        for (BytecodeExpression e : arguments) {
                            code.exec(e);
                        }
                        MethodVisitor mv = code.getMethodVisitor();
                        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(clazz), "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, args), false);
                    }
                }
        );
    }

    @Override
    public Invocable invoker() {
        return generator.createInvocable();
    }
}
