/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.EvaluatedExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

final class EnumConstant extends BaseTypeExpression implements EvaluatedExpression {
    private final Enum value;

    EnumConstant(TypeWidget type, Enum value) {
        super(type);
        this.value = value;
    }

    @Override
    public void generate(CodeEmitter environment) {
        environment.getMethodVisitor().visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(value.getDeclaringClass()), value.name(), Type.getDescriptor(value.getDeclaringClass()));
    }
}
