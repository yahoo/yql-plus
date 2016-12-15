/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class JacksonSerializeObject implements BytecodeSequence {
    private final BytecodeExpression source;
    private final BytecodeExpression jsonGenerator;

    public JacksonSerializeObject(BytecodeExpression source, BytecodeExpression jsonGenerator) {
        this.source = source;
        this.jsonGenerator = jsonGenerator;
    }

    @Override
    public void generate(CodeEmitter code) {
        jsonGenerator.generate(code);
        source.generate(code);
        MethodVisitor mv = code.getMethodVisitor();
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JsonGenerator.class), "writeObject", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Object.class)), false);
    }
}
