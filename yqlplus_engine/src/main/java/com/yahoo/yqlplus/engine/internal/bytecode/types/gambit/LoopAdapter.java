/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.bytecode.ASMClassSource;
import com.yahoo.yqlplus.engine.internal.compiler.ChunkExpression;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.LocalCodeChunk;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class LoopAdapter extends ExpressionHandler implements GambitCreator.LoopBuilder {
    private final BytecodeExpression result;
    private final Label next;

    public LoopAdapter(ASMClassSource source, LocalCodeChunk parent, final BytecodeExpression test, BytecodeExpression result) {
        super(source);
        this.body = parent.child();
        this.result = result;
        next = body.getStart();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                abort(test);
            }
        });
        LocalCodeChunk child = body.block();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.getMethodVisitor().visitJumpInsn(Opcodes.GOTO, body.getStart());
            }
        });
        body = child;
    }

    @Override
    public void abort(final BytecodeExpression test) {
        final TypeWidget type = test.getType();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(test);
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


    @Override
    public void next(final BytecodeExpression test) {
        final TypeWidget type = test.getType();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(test);
                Label isTrue = new Label();
                Label isFalse = new Label();
                type.getComparisionAdapter().coerceBoolean(code, isTrue, isFalse, isFalse);
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitLabel(isTrue);
                code.exec(result);
                mv.visitJumpInsn(Opcodes.GOTO, next);
                mv.visitLabel(isFalse);
            }
        });

    }

    @Override
    public BytecodeExpression build() {
        body.add(result);
        return new ChunkExpression(result.getType(), body);
    }
}
