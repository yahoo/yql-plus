/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Modifier;
import java.util.List;

public abstract class FunctionGenerator extends Annotatable implements LocalCodeChunk {
    protected int modifiers = Modifier.PUBLIC | Modifier.FINAL;

    protected final UnitGenerator unit;
    protected final LocalFrame intrinsic;
    protected final LocalFrame arguments;
    protected final LocalCodeChunk code;

    protected FunctionGenerator(UnitGenerator unit, boolean isStatic) {
        super(unit.getEnvironment());
        this.unit = unit;
        this.intrinsic = new LocalFrame();
        if (isStatic) {
            addModifier(Opcodes.ACC_STATIC);
        } else {
            intrinsic.allocate("this", unit.getType());
        }
        this.arguments = new LocalFrame(intrinsic);
        this.code = new LocalBatchCode(arguments);
    }

    public AssignableValue addArgument(String name, TypeWidget type) {
        return arguments.allocate(name, type);
    }

    public abstract void generate(ClassVisitor cw);

    public void addModifier(int elt) {
        modifiers |= elt;
    }

    public void setModifier(int modifiers) {
        this.modifiers = modifiers;
    }

    public List<TypeWidget> getArgumentTypes() {
        return arguments.getTypes();
    }

    protected Type getReturnJVMType() {
        return Type.VOID_TYPE;
    }

    public String createMethodDescriptor() {
        List<TypeWidget> argumentTypes = getArgumentTypes();
        Type[] argTypes = new Type[argumentTypes.size()];
        int i = 0;
        for (TypeWidget t : argumentTypes) {
            argTypes[i++] = t.getJVMType();
        }
        return Type.getMethodDescriptor(getReturnJVMType(), argTypes);
    }

    @Override
    public Label getStart() {
        return code.getStart();
    }

    @Override
    public Label getEnd() {
        return code.getEnd();
    }

    @Override
    public void execute(BytecodeSequence code) {
        this.code.execute(code);
    }

    @Override
    public LocalCodeChunk block() {
        return code.block();
    }

    @Override
    public LocalCodeChunk point() {
        return code.point();
    }

    @Override
    public LocalCodeChunk child() {
        return code.child();
    }

    @Override
    public void add(BytecodeSequence code) {
        this.code.add(code);
    }

    @Override
    public void generate(CodeEmitter code) {
        this.code.generate(code);
    }

    @Override
    public AssignableValue allocate(TypeWidget type) {
        return code.allocate(type);
    }

    @Override
    public AssignableValue evaluate(BytecodeExpression expr) {
        return code.evaluate(expr);
    }

    @Override
    public AssignableValue allocate(String name, TypeWidget type) {
        return code.allocate(name, type);
    }

    @Override
    public AssignableValue evaluate(String name, BytecodeExpression expr) {
        return code.evaluate(name, expr);
    }

    @Override
    public AssignableValue getLocal(String name) {
        return code.getLocal(name);
    }

    @Override
    public void alias(String from, String to) {
        code.alias(from, to);
    }

    public abstract GambitCreator.Invocable createInvocable();
}
