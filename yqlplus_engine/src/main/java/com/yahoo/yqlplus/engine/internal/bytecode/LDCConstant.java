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
import org.objectweb.asm.MethodVisitor;

class LDCConstant extends BaseTypeExpression implements EvaluatedExpression {
    protected Object constant;

    LDCConstant(TypeWidget type, Object constant) {
        super(type);
        this.constant = constant;
    }

    @Override
    public void generate(CodeEmitter environment) {
        MethodVisitor mv = environment.getMethodVisitor();
        if (constant instanceof Integer) {
            environment.emitIntConstant((int) constant);
        } else {
            mv.visitLdcInsn(constant);
        }
    }
}
