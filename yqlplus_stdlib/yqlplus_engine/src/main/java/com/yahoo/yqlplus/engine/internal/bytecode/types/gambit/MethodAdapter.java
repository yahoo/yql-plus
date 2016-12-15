/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.bytecode.ReturnCode;
import com.yahoo.yqlplus.engine.internal.compiler.MethodGenerator;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

import java.lang.annotation.Annotation;


public class MethodAdapter extends ExpressionHandler implements ObjectBuilder.MethodBuilder {
    private MethodGenerator generator;

    public MethodAdapter(ASMClassSource source, MethodGenerator generator) {
        super(source);
        this.generator = generator;
        body = generator;
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        return generator.addArgument(name, type).read();
    }

    @Override
    public void exit(BytecodeExpression result) {
        generator.add(result);
        generator.add(new ReturnCode(result.getType()));
        generator.setReturnType(result.getType());
    }

    @Override
    public void exit() {
        generator.add(new ReturnCode());
    }

    @Override
    public Invocable invoker(final BytecodeExpression target) {
        return generator.createInvocable().prefix(target);
    }

    @Override
    public Invocable invoker() {
        return generator.createInvocable();
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
    public ObjectBuilder.AnnotationBuilder annotate(org.objectweb.asm.Type type) {
        return generator.annotate(type);
    }

    @Override
    public void annotate(Annotation annotation) {
        generator.annotate(annotation);
    }
}
