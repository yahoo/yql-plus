/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.exprs;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.List;

public class MulticompareExpression extends BaseTypeExpression {
    private final List<BytecodeExpression> expressions;

    public MulticompareExpression(List<BytecodeExpression> expressions) {
        super(BaseTypeAdapter.INT32);
        this.expressions = expressions;
    }

    @Override
    public void generate(CodeEmitter code) {
        CodeEmitter scope = code.createScope();
        MethodVisitor mv = scope.getMethodVisitor();
        AssignableValue var = scope.allocate(BaseTypeAdapter.INT32);
        for (BytecodeExpression expr : expressions) {
            Label areEqual = new Label();
            scope.exec(var.write(expr));
            scope.exec(var.read());
            mv.visitJumpInsn(Opcodes.IFEQ, areEqual);
            scope.exec(var.read());
            scope.gotoExitScope();
            mv.visitLabel(areEqual);
        }
        mv.visitInsn(Opcodes.ICONST_0);
        scope.endScope();
    }
}
