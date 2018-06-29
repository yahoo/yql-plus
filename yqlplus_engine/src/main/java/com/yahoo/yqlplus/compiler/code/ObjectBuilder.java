/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.util.List;


public interface ObjectBuilder {
    Type getJVMType();

    void setTypeWidget(TypeWidget overrideTypeWidget);

    interface AnnotationBuilder {
        Object put(String key, Object value);

        Object get(Object key);
    }

    interface Annotatable {
        AnnotationBuilder annotate(Class<?> clazz);

        AnnotationBuilder annotate(Type type);

        void annotate(Annotation annotation);
    }

    interface MemberBuilder extends Annotatable {
        void setModifiers(int modifiers);

        void addModifiers(int modifiers);
    }

    interface FieldBuilder extends MemberBuilder {
        AssignableValue get(BytecodeExpression target);

        TypeWidget getType();
    }

    interface MethodBuilder extends ScopedBuilder, MemberBuilder {
        BytecodeExpression addArgument(String name, TypeWidget type);

        void exit(BytecodeExpression result);

        void exit();

        Invocable invoker(BytecodeExpression target);

        Invocable invoker();
    }

    interface ConstructorBuilder extends ScopedBuilder, MemberBuilder {
        BytecodeExpression addArgument(String name, TypeWidget type);

        void invokeSpecial(Class<?> clazz, BytecodeExpression... arguments);

        void invokeSpecial(Class<?> clazz, List<BytecodeExpression> arguments);

        Invocable invoker();
    }

    ConstructorBuilder getConstructor();

    void addParameter(String name, TypeWidget type);

    void addParameterField(FieldBuilder field);

    void implement(Class<?> clazz);

    MethodBuilder method(String name);

    GambitCreator.Invocable methodInvocable(String name, BytecodeExpression target);

    GambitCreator.Invocable staticInvocable(String name);

    boolean hasMethod(String name);

    MethodBuilder staticMethod(String name);

    FieldBuilder field(String name, TypeWidget type);

    FieldBuilder finalField(String name, BytecodeExpression finalValue);

    TypeWidget type();
}
