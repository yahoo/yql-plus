/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.bytecode.FieldDefinition;
import com.yahoo.yqlplus.engine.internal.bytecode.UnitGenerator;
import com.yahoo.yqlplus.engine.internal.compiler.ConstructorGenerator;
import com.yahoo.yqlplus.engine.internal.compiler.LocalCodeChunk;
import com.yahoo.yqlplus.engine.internal.compiler.MethodGenerator;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NotNullableTypeWidget;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Modifier;
import java.util.List;

public class GambitUnit extends UnitGenerator implements ObjectBuilder {
    private List<FieldDefinition> argumentFields;
    private ConstructorGenerator constructor;
    private ConstructorAdapter constructorAdapter;
    private List<LocalCodeChunk> methodPreambles;

    public GambitUnit(String name, ASMClassSource environment) {
        super(name, environment);
        init();
    }

    public GambitUnit(String name, Class<?> superClazz, ASMClassSource environment) {
        super(name, superClazz, environment);
        init();
    }


    public GambitUnit(String name, String superClassName, ASMClassSource environment) {
        super(name, superClassName, environment);
        init();
    }

    private void init() {
        constructor = createConstructor();
        constructorAdapter = new ConstructorAdapter(environment, this, constructor);
        argumentFields = Lists.newArrayList();
        methodPreambles = Lists.newArrayList();
    }


    @Override
    public void addParameter(String name, TypeWidget type) {
        AssignableValue va = constructor.addArgument(name, type);
        FieldDefinition defn = createField(name, va.read());
        defn.addModifier(Modifier.FINAL);
        addParameterField(defn);
    }

    @Override
    public void addParameterField(FieldBuilder builder) {
        FieldDefinition defn = (FieldDefinition) builder;
        argumentFields.add(defn);
        for (LocalCodeChunk preamble : methodPreambles) {
            preamble.evaluate(defn.getName(), defn.get(preamble.getLocal("this")).read());
        }
    }

    protected MethodGenerator initMethodLocals(MethodGenerator locals) {
        LocalCodeChunk preamble = locals.point();
        for (FieldDefinition defn : argumentFields) {
            preamble.evaluate(defn.getName(), defn.get(preamble.getLocal("this")).read());
        }
        methodPreambles.add(preamble);
        return locals;
    }

    @Override
    public ConstructorBuilder getConstructor() {
        return constructorAdapter;
    }

    @Override
    public MethodGenerator createMethod(String name) {
        return initMethodLocals(super.createMethod(name));
    }

    @Override
    public MethodBuilder method(String name) {
        return new MethodAdapter(environment, createMethod(name));
    }

    @Override
    public MethodBuilder staticMethod(String name) {
        return new MethodAdapter(environment, createStaticMethod(name));
    }

    @Override
    public GambitCreator.Invocable methodInvocable(String name, BytecodeExpression target) {
        Preconditions.checkArgument(methods.containsKey(name));
        return new MethodAdapter(environment, getMethod(name)).invoker(target);
    }

    @Override
    public GambitCreator.Invocable staticInvocable(String name) {
        Preconditions.checkArgument(methods.containsKey(name));
        return new MethodAdapter(environment, getMethod(name)).invoker();
    }

    @Override
    public void implement(Class<?> clazz) {
        addInterface(clazz);
    }

    @Override
    public FieldBuilder field(String name, TypeWidget type) {
        return createField(type, name);
    }

    @Override
    public FieldBuilder finalField(String name, BytecodeExpression finalValue) {
        FieldBuilder field = createField(name, finalValue);
        field.addModifiers(Opcodes.ACC_FINAL);
        return field;
    }

    @Override
    public TypeWidget type() {
        return NotNullableTypeWidget.create(getType());
    }

    @Override
    public Type getJVMType() {
        return getType().getJVMType();
    }
}
