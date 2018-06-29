/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;

import java.lang.reflect.Modifier;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.RETURN;

public class ConstructorGenerator extends FunctionGenerator {
    public ConstructorGenerator(UnitGenerator unit) {
        super(unit, false);
        this.modifiers = Modifier.PUBLIC;
    }

    private MethodVisitor createMethod(ClassVisitor cw) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", createMethodDescriptor(), null, null);
        generateAnnotations(mv);
        mv.visitCode();
        return mv;
    }

    @Override
    public void generate(ClassVisitor cw) {
        CodeEmitter out = new UnitCodeEmitter(unit, arguments, createMethod(cw));
        MethodVisitor mv = out.getMethodVisitor();
        unit.getSuperInit().generate(out);
        code.generate(out);
        mv.visitInsn(RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    @Override
    public GambitCreator.Invocable createInvocable() {
        return ConstructInvocation.constructor(unit.getType(), getArgumentTypes());
    }
}
