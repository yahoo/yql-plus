/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.exprs.ChunkExpression;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.LocalCodeChunk;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.List;

public class CaseAdapter extends ExpressionHandler implements GambitCreator.CaseBuilder {
    private final Label end;
    private final List<TypeWidget> resultTypes = Lists.newArrayList();
    private TypeWidget unifiedOutputType;

    public CaseAdapter(ASMClassSource source, LocalCodeChunk parent) {
        super(source);
        this.body = parent.child();
        end = body.getEnd();
    }

    @Override
    public void when(final BytecodeExpression test, final BytecodeExpression value) {
        final TypeWidget type = test.getType();
        resultTypes.add(value.getType());
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(test);
                Label isTrue = new Label();
                Label isFalse = new Label();
                type.getComparisionAdapter().coerceBoolean(code, isTrue, isFalse, isFalse);
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitLabel(isTrue);
                code.exec(value);
                code.cast(unifiedOutputType, value.getType());
                mv.visitJumpInsn(Opcodes.GOTO, end);
                mv.visitLabel(isFalse);
            }
        });
    }

    @Override
    public BytecodeExpression exit(final BytecodeExpression defaultCase) {
        Preconditions.checkState(unifiedOutputType == null, "Invalid state: IfAdapter.exit called twice");
        resultTypes.add(defaultCase.getType());
        unifiedOutputType = unify(resultTypes);
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(defaultCase);
                code.cast(unifiedOutputType, defaultCase.getType());
            }
        });
        return new ChunkExpression(unifiedOutputType, body);
    }
}
