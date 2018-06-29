/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.code.LocalCodeChunk;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class IfAdapter extends ExpressionHandler implements GambitCreator.IfBuilder {
    private final Label end;
    private final Clause elseIf;
    private LocalCodeChunk statement;

    public IfAdapter(ASMClassSource source, LocalCodeChunk parent) {
        super(source);
        this.statement = parent.child();
        this.body = statement.block();
        end = statement.getEnd();
        this.elseIf = new Clause(source, statement);
    }

    private static class Clause extends ExpressionHandler {
        public Clause(ASMClassSource source, LocalCodeChunk body) {
            super(source);
            this.body = body;
        }
    }


    @Override
    public ScopedBuilder when(final BytecodeExpression test) {
        final TypeWidget type = test.getType();
        final Label isFalse = new Label();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(test);
                Label isTrue = new Label();
                type.getComparisionAdapter().coerceBoolean(code, isTrue, isFalse, isFalse);
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitLabel(isTrue);
            }
        });
        Clause block = new Clause(source, body.block());
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitJumpInsn(Opcodes.GOTO, end);
                mv.visitLabel(isFalse);
            }
        });
        return block;
    }

    @Override
    public ScopedBuilder elseIf() {
        return elseIf;
    }

    @Override
    public BytecodeSequence build() {
        return statement;
    }
}
