/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

public class IterateBuilderAdapter extends ExpressionHandler implements GambitCreator.IterateBuilder {
    private final BytecodeExpression itemExpr;
    private final LocalCodeChunk block;

    private Label next;
    private Label abort;

    public IterateBuilderAdapter(ASMClassSource source, LocalCodeChunk parent, BytecodeExpression iterable) {
        super(source);
        block = parent.child();
        body = block.child();
        final TypeWidget type = iterable.getType();
        final AssignableValue av = block.allocate(type.getIterableAdapter().getValue());
        this.itemExpr = av.read();
        block.add(type.getIterableAdapter()
                .iterate(iterable, av, new IterateAdapter.IterateLoop() {
                            @Override
                            public void item(CodeEmitter code, BytecodeExpression item, Label abortLoop, Label nextItem) {
                                IterateBuilderAdapter.this.next = nextItem;
                                IterateBuilderAdapter.this.abort = abortLoop;
                                body.generate(code);
                            }
                        }
                )
        );
    }

    @Override
    public void abort(final BytecodeExpression test) {
        final TypeWidget type = test.getType();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(test);
                Label isFalse = new Label();
                type.getComparisionAdapter().coerceBoolean(code, abort, isFalse, isFalse);
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitLabel(isFalse);
            }
        });
    }

    @Override
    public void next(BytecodeExpression test) {
        final BytecodeExpression testExpr = test;
        final TypeWidget type = testExpr.getType();
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(testExpr);
                Label isFalse = new Label();
                type.getComparisionAdapter().coerceBoolean(code, next, isFalse, isFalse);
                MethodVisitor mv = code.getMethodVisitor();
                mv.visitLabel(isFalse);
            }
        });

    }

    @Override
    public BytecodeExpression getItem() {
        return itemExpr;
    }

    @Override
    public BytecodeExpression build(final BytecodeExpression result) {
        block.add(result);
        return new ChunkExpression(result.getType(), block);
    }

    @Override
    public BytecodeExpression build() {
        return new ChunkExpression(BaseTypeAdapter.VOID, block);
    }
}
