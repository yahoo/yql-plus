/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.yahoo.yqlplus.compiler.exprs.NullExpr;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.MethodGenerator;
import com.yahoo.yqlplus.compiler.generate.UnitGenerator;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.types.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

public class CodeEmitterTest extends PhysicalExpressionCompilerTest {
    @Test
    public void requireSimpleAdd() throws Exception {
        UnitGenerator unit = new UnitGenerator("foo", source);
        unit.addInterface(Callable.class);
        MethodGenerator gen = unit.createMethod("call");
        gen.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression leftExpr = source.constant(1);
        final BytecodeExpression rightExpr = source.constant(1);

        gen.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label hasNull = new Label();
                CodeEmitter.Unification unify = code.unifiedEmit(leftExpr, rightExpr, hasNull);
                code.getMethodVisitor().visitInsn(Opcodes.IADD);
                code.box(BaseTypeAdapter.INT32);
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                if (unify.nullPossible) {
                    code.getMethodVisitor().visitLabel(hasNull);
                    code.getMethodVisitor().visitInsn(Opcodes.ACONST_NULL);
                    code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                }
            }
        });

        source.build();
        Class<? extends Callable> clazz = (Class<? extends Callable>) source.getGeneratedClass(unit);
        Callable c = clazz.newInstance();
        Assert.assertEquals(2, c.call());
    }

    @Test
    public void requireWidenAdd() throws Exception {
        UnitGenerator unit = new UnitGenerator("foo", source);
        unit.addInterface(Callable.class);
        MethodGenerator gen = unit.createMethod("call");
        gen.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression leftExpr = source.constant(1);
        final BytecodeExpression rightExpr = source.constant(1L);

        gen.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label hasNull = new Label();
                CodeEmitter.Unification unify = code.unifiedEmit(leftExpr, rightExpr, hasNull);
                code.getMethodVisitor().visitInsn(Opcodes.LADD);
                code.box(BaseTypeAdapter.INT64);
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                if (unify.nullPossible) {
                    code.getMethodVisitor().visitLabel(hasNull);
                    code.getMethodVisitor().visitInsn(Opcodes.ACONST_NULL);
                    code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                }
            }
        });

        source.build();
        Class<? extends Callable> clazz = (Class<? extends Callable>) source.getGeneratedClass(unit);
        Callable c = clazz.newInstance();
        Assert.assertEquals(2L, c.call());
    }

    @Test
    public void requireFloatAdd() throws Exception {
        UnitGenerator unit = new UnitGenerator("foo", source);
        unit.addInterface(Callable.class);
        MethodGenerator gen = unit.createMethod("call");
        gen.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression leftExpr = source.constant(1);
        final BytecodeExpression rightExpr = source.constant(1.0f);

        gen.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label hasNull = new Label();
                CodeEmitter.Unification unify = code.unifiedEmit(leftExpr, rightExpr, hasNull);
                code.getMethodVisitor().visitInsn(Opcodes.DADD);
                code.box(BaseTypeAdapter.FLOAT64);
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                if (unify.nullPossible) {
                    code.getMethodVisitor().visitLabel(hasNull);
                    code.getMethodVisitor().visitInsn(Opcodes.ACONST_NULL);
                    code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                }
            }
        });

        source.build();
        Class<? extends Callable> clazz = (Class<? extends Callable>) source.getGeneratedClass(unit);
        Callable c = clazz.newInstance();
        Assert.assertEquals(2.0, c.call());
    }

    @Test
    public void requireBoxedAdd() throws Exception {
        UnitGenerator unit = new UnitGenerator("foo", source);
        unit.addInterface(Callable.class);
        MethodGenerator gen = unit.createMethod("call");
        gen.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression leftExpr = source.constant(BaseTypeAdapter.INT32.boxed(), 1);
        final BytecodeExpression rightExpr = source.constant(1.0f);

        gen.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label hasNull = new Label();
                CodeEmitter.Unification unify = code.unifiedEmit(leftExpr, rightExpr, hasNull);
                code.getMethodVisitor().visitInsn(Opcodes.DADD);
                code.box(BaseTypeAdapter.FLOAT64);
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                if (unify.nullPossible) {
                    code.getMethodVisitor().visitLabel(hasNull);
                    code.getMethodVisitor().visitInsn(Opcodes.ACONST_NULL);
                    code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                }
            }
        });

        source.build();
        Class<? extends Callable> clazz = (Class<? extends Callable>) source.getGeneratedClass(unit);
        Callable c = clazz.newInstance();
        Assert.assertEquals(2.0, c.call());
    }

    @Test
    public void requireBoxedNull() throws Exception {
        UnitGenerator unit = new UnitGenerator("foo", source);
        unit.addInterface(Callable.class);
        MethodGenerator gen = unit.createMethod("call");
        gen.setReturnType(AnyTypeWidget.getInstance());
        final BytecodeExpression rightExpr = source.constant(1.0f);

        gen.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label hasNull = new Label();
                CodeEmitter.Unification unify = code.unifiedEmit(new NullExpr(BaseTypeAdapter.INT32.boxed()), rightExpr, hasNull);
                code.getMethodVisitor().visitInsn(Opcodes.DADD);
                code.box(BaseTypeAdapter.FLOAT64);
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                if (unify.nullPossible) {
                    code.getMethodVisitor().visitLabel(hasNull);
                    code.getMethodVisitor().visitInsn(Opcodes.ACONST_NULL);
                    code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
                }
            }
        });

        source.build();
        Class<? extends Callable> clazz = (Class<? extends Callable>) source.getGeneratedClass(unit);
        Callable c = clazz.newInstance();
        Assert.assertNull(c.call());
    }
}
