/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.LocalCodeChunk;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class BlockAdapter extends ExpressionHandler implements GambitCreator.ScopeBuilder {
    public BlockAdapter(ASMClassSource source, LocalCodeChunk body) {
        super(source);
        this.body = body;
    }

    @Override
    public BytecodeExpression complete(BytecodeExpression end) {
        body.add(end);
        return new BaseTypeExpression((TypeWidget) end.getType()) {
            @Override
            public void generate(CodeEmitter code) {
                body.generate(code);
            }
        };
    }

    @Override
    public void jump(BytecodeExpression test, final BytecodeExpression result) {
        final BytecodeExpression testExpr = test;
        final TypeWidget type = testExpr.getType();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(testExpr);
                Label isTrue = new Label();
                Label isFalse = new Label();
                type.getComparisionAdapter().coerceBoolean(code, isTrue, isFalse, isFalse);
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitLabel(isTrue);
                code.exec(result);
                mv.visitJumpInsn(Opcodes.GOTO, body.getEnd());
                mv.visitLabel(isFalse);
            }
        });
    }
}
