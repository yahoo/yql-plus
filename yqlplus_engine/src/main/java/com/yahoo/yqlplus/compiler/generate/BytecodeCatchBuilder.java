/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.LocalCodeChunk;
import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.List;

public class BytecodeCatchBuilder implements GambitCreator.CatchBuilder {
    private final ASMClassSource source;
    private final LocalCodeChunk parent;
    private final Clause body;
    private final List<CatchClause> catchClauses;

    public BytecodeCatchBuilder(ASMClassSource source, LocalCodeChunk parent) {
        this.source = source;
        this.parent = parent;
        this.body = new Clause(source, parent.child());
        this.catchClauses = Lists.newArrayList();
    }

    private static class Clause extends ExpressionHandler implements BytecodeSequence {
        public Clause(ASMClassSource source, LocalCodeChunk body) {
            super(source);
            this.body = body;
        }

        @Override
        public void generate(CodeEmitter code) {
            body.generate(code);
        }
    }

    private static class CatchClause extends Clause {
        final List<String> exceptionInternalNames;

        private CatchClause(ASMClassSource source, LocalCodeChunk parent) {
            super(source, parent);
            this.exceptionInternalNames = Lists.newArrayList();
        }
    }

    @Override
    public ScopedBuilder body() {
        return body;
    }

    @Override
    public ScopedBuilder on(String varName, TypeWidget exceptionType, TypeWidget... moreExceptionTypes) {
        CatchClause catchClause = new CatchClause(source, parent.child());
        for (TypeWidget t : Lists.asList(exceptionType, moreExceptionTypes)) {
            catchClause.exceptionInternalNames.add(t.getJVMType().getInternalName());
            exceptionType = source.getValueTypeAdapter().unifyTypes(exceptionType, t);
        }
        AssignableValue local = catchClause.allocate(varName, exceptionType);
        catchClause.body.add(local.write(exceptionType));
        catchClauses.add(catchClause);
        return catchClause;
    }

    @Override
    public ScopedBuilder on(String varName, Class<?> exceptionType, Class<?>... moreExceptionTypes) {
        CatchClause catchClause = new CatchClause(source, parent.child());
        TypeWidget unifiedType = source.getValueTypeAdapter().adaptInternal(exceptionType);
        for (Class<?> c : Lists.asList(exceptionType, moreExceptionTypes)) {
            TypeWidget t = source.getValueTypeAdapter().adaptInternal(c);
            catchClause.exceptionInternalNames.add(t.getJVMType().getInternalName());
            unifiedType = source.getValueTypeAdapter().unifyTypes(unifiedType, t);
        }
        AssignableValue local = catchClause.allocate(varName, unifiedType);
        catchClause.body.add(local.write(unifiedType));
        catchClauses.add(catchClause);
        return catchClause;
    }

    @Override
    public ScopedBuilder always() {
        CatchClause catchClause = new CatchClause(source, parent.child()) {
            @Override
            public void generate(CodeEmitter code) {
                super.generate(code);
                code.getMethodVisitor().visitInsn(Opcodes.ATHROW);
            }
        };
        catchClauses.add(catchClause);
        return catchClause;
    }

    @Override
    public BytecodeSequence build() {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                MethodVisitor mv = code.getMethodVisitor();
                for (CatchClause catchClause : catchClauses) {
                    if (catchClause.exceptionInternalNames.isEmpty()) {
                        mv.visitTryCatchBlock(body.getStart(), body.getEnd(), catchClause.getStart(), null);
                    } else {
                        for (String name : catchClause.exceptionInternalNames) {
                            mv.visitTryCatchBlock(body.getStart(), body.getEnd(), catchClause.getStart(), name);
                        }
                    }
                }
                Label done = new Label();
                body.generate(code);
                mv.visitJumpInsn(Opcodes.GOTO, done);
                for (CatchClause catchClause : catchClauses.subList(0, catchClauses.size() - 1)) {
                    catchClause.generate(code);
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                }
                catchClauses.get(catchClauses.size() - 1).generate(code);
                mv.visitLabel(done);
            }
        };
    }
}
