/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.google.common.base.Predicate;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.types.YQLBaseType;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.engine.compiler.code.ASMClassSource;
import com.yahoo.yqlplus.engine.compiler.code.AssignableValue;
import com.yahoo.yqlplus.engine.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.compiler.code.BaseTypeExpression;
import com.yahoo.yqlplus.engine.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.engine.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.engine.compiler.code.ConstructorGenerator;
import com.yahoo.yqlplus.engine.compiler.code.FieldDefinition;
import com.yahoo.yqlplus.engine.compiler.code.MethodGenerator;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.compiler.code.UnitGenerator;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;

public class ASMClassSourceTest {

    static class ToyClass extends UnitGenerator {
        ToyClass(String name, ASMClassSource environment) {
            super(name, environment);
            addInterface(Computor.class);
        }
    }

    @Test
    public void requireCreateClass() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ASMClassSource source = createASMClassSource();
        ToyClass toy = new ToyClass("toy", source);
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.emitIntConstant(0);
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        Computor foo = clazz.newInstance();
        Assert.assertEquals(foo.compute(), 0);
    }

    private ASMClassSource createASMClassSource() {
        return new ASMClassSource();
    }

    @Test
    public void requireCreateSuperClass() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ASMClassSource source = createASMClassSource();
        UnitGenerator toy = new UnitGenerator("base", ComputorBase.class, source) {
        };
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.emitIntConstant(1);
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        Computor foo = clazz.newInstance();
        Assert.assertEquals(foo.compute(), 1);
    }

    @Test
    public void requireFields() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ASMClassSource source = createASMClassSource();
        final ToyClass toy = new ToyClass("toy", source);
        toy.createField("a", source.constant(BaseTypeAdapter.INT32, 1));
        toy.createField("b", source.constant(BaseTypeAdapter.INT32, 2));
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                toy.getField("a").get(code.getLocal("this").read()).write(
                        new BaseTypeExpression(BaseTypeAdapter.INT32) {
                            @Override
                            public void generate(CodeEmitter code) {
                                toy.getField("a").get(code.getLocal("this").read()).read().generate(code);
                                code.emitIntConstant(1);
                                code.getMethodVisitor().visitInsn(Opcodes.IADD);
                            }
                        }
                ).generate(code);
                toy.getField("a").get(code.getLocal("this").read()).read().generate(code);
                toy.getField("b").get(code.getLocal("this").read()).read().generate(code);
                code.getMethodVisitor().visitInsn(Opcodes.IADD);
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        try {
            Computor foo = clazz.newInstance();
            Assert.assertEquals(foo.compute(), 4);
            Assert.assertEquals(foo.compute(), 5);
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }

    @Test
    public void requireBoxedConstant() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        final ASMClassSource source = createASMClassSource();
        ToyClass toy = new ToyClass("toy", source);
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                source.constant(BaseTypeAdapter.INT32.boxed(), 1).generate(code);
                code.unbox(BaseTypeAdapter.INT32.boxed());
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        Computor foo = clazz.newInstance();
        Assert.assertEquals(foo.compute(), 1);
    }

    @Test
    public void requireInjectedConstant() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ASMClassSource source = createASMClassSource();
        final ToyClass toy = new ToyClass("toy", source);
        toy.createField("a", source.constant(source.adaptInternal(Hat.class), new Hat()));
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                toy.getField("a").get(code.getLocal("this").read()).read().generate(code);
                code.getMethodVisitor().visitFieldInsn(Opcodes.GETFIELD, Type.getInternalName(Hat.class), "VALUE", Type.INT_TYPE.getDescriptor());
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        try {
            Computor foo = clazz.newInstance();
            Assert.assertEquals(foo.compute(), 1);
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }

    @Test
    public void testStruct() throws Exception {
        YQLStructType structType = YQLStructType.builder()
                .addField("a", YQLBaseType.INT32)
                .build();
        final ASMClassSource source = createASMClassSource();
        final TypeWidget struct = source.resolveStruct(structType);

        final ToyClass toy = new ToyClass("toy", source);
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                AssignableValue local = code.allocate(struct, "s");
                local.write(struct.construct()).generate(code);
                AssignableValue property = struct.getPropertyAdapter().property(local.read(), "a");
                property.write(source.constant(BaseTypeAdapter.INT32, 1)).generate(code);
                property.read().generate(code);
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        try {
            Computor foo = clazz.newInstance();
            Assert.assertEquals(foo.compute(), 1);
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }

    @Test
    public void requireEnumConstant() throws Exception {
        final ASMClassSource source = createASMClassSource();
        final UnitGenerator toy = new UnitGenerator("toy", source);
        toy.addInterface(Excellentor.class);
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(source.adaptInternal(Excellent.class));
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                source.constant(source.adaptInternal(Excellent.class), Excellent.BOGUS).generate(code);
                code.getMethodVisitor().visitInsn(Opcodes.ARETURN);
            }
        });
        source.build();
        Class<? extends Excellentor> clazz = (Class<? extends Excellentor>) toy.getGeneratedClass();
        try {
            Excellentor foo = clazz.newInstance();
            Assert.assertEquals(foo.compute(), Excellent.BOGUS);
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }


    @Test
    public void requireInjector() throws Exception {
        final ASMClassSource source = createASMClassSource();
        final UnitGenerator toy = new UnitGenerator("toy", source);
        toy.addInterface(Computor.class);
        FieldDefinition field = toy.createField(source.adaptInternal(Hat.class), "hat");
        field.addModifier(Modifier.FINAL);
        ConstructorGenerator gen = toy.createConstructor();
        gen.annotate(Inject.class);
        AssignableValue thisValue = gen.getLocal("this");
        AssignableValue hatArg = gen.addArgument("hat", source.adaptInternal(Hat.class));
        gen.add(field.get(thisValue.read()).write(hatArg.read()));
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                toy.getField("hat").get(code.getLocal("this").read()).read().generate(code);
                code.getMethodVisitor().visitFieldInsn(Opcodes.GETFIELD, Type.getInternalName(Hat.class), "VALUE", Type.INT_TYPE.getDescriptor());
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Hat.class).toInstance(new Hat());
            }
        });
        try {
            Computor foo = injector.getInstance(clazz);
            Assert.assertEquals(foo.compute(), 1);
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }

    @Test
    public void requireInjectorField() throws Exception {
        final ASMClassSource source = createASMClassSource();
        final UnitGenerator toy = new UnitGenerator("toy", source);
        toy.addInterface(Computor.class);
        FieldDefinition field = toy.createField(source.adaptInternal(Hat.class), "hat");
        field.addModifier(Modifier.FINAL);
        field.annotate(Inject.class);
        MethodGenerator method = toy.createMethod("compute");
        method.setReturnType(BaseTypeAdapter.INT32);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                toy.getField("hat").get(code.getLocal("this").read()).read().generate(code);
                code.getMethodVisitor().visitFieldInsn(Opcodes.GETFIELD, Type.getInternalName(Hat.class), "VALUE", Type.INT_TYPE.getDescriptor());
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Computor> clazz = (Class<? extends Computor>) toy.getGeneratedClass();
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Hat.class).toInstance(new Hat());
            }
        });
        try {
            Computor foo = injector.getInstance(clazz);
            Assert.assertEquals(foo.compute(), 1);
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }

    @Test
    public void requirePredicate() throws Exception {
        final ASMClassSource source = createASMClassSource();
        final UnitGenerator toy = new UnitGenerator("toy", source);
        toy.addInterface(Predicate.class);
        MethodGenerator method = toy.createMethod("apply");
        method.setReturnType(BaseTypeAdapter.BOOLEAN);
        method.addArgument("input", BaseTypeAdapter.ANY);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                source.constant(BaseTypeAdapter.BOOLEAN, true).generate(code);
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Predicate> clazz = (Class<? extends Predicate>) toy.getGeneratedClass();
        try {
            Predicate<Object> foo = clazz.newInstance();
            Assert.assertTrue(foo.apply(1));
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }

    @Test
    public void requirePredicateBoxed() throws Exception {
        final ASMClassSource source = createASMClassSource();
        final UnitGenerator toy = new UnitGenerator("toy", source);
        toy.addInterface(Predicate.class);
        MethodGenerator method = toy.createMethod("apply");
        method.setReturnType(BaseTypeAdapter.BOOLEAN);
        method.addArgument("input", BaseTypeAdapter.ANY);
        method.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                source.constant(BaseTypeAdapter.BOOLEAN.boxed(), true).generate(code);
                code.unbox(BaseTypeAdapter.BOOLEAN.boxed());
                code.getMethodVisitor().visitInsn(Opcodes.IRETURN);
            }
        });
        source.build();
        Class<? extends Predicate> clazz = (Class<? extends Predicate>) toy.getGeneratedClass();
        try {
            Predicate<Object> foo = clazz.newInstance();
            Assert.assertTrue(foo.apply(1));
        } catch (VerifyError e) {
            toy.trace(System.err);
            throw e;
        }
    }
    
    //This test shows inconsistent int values of true may cause hotspot problem
    //Doesn't have to be a real test. Won't enable it
    /*
    @Test
    public void requireBooleanCoerceTestClass() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
      ASMClassSource source = createASMClassSource();
      BooleanCoerce booleanCoerce = new BooleanCoerce("booleanCoerce", source);
      MethodGenerator method = booleanCoerce.createStaticMethod("invoke");
      method.setReturnType(BaseTypeAdapter.BOOLEAN);
      method.add(new BytecodeSequence() {
        @Override
        public void generate(CodeEmitter code) {
          Label done = new Label();
          Label isTrue = new Label();
          Label isFalse = new Label();
          
          MethodVisitor mv = code.getMethodVisitor();
          mv.visitInsn(Opcodes.ICONST_1);
          mv.visitJumpInsn(Opcodes.IFEQ, isFalse);
          mv.visitJumpInsn(Opcodes.GOTO, isTrue);
          mv.visitLabel(isFalse);
          mv.visitInsn(Opcodes.ICONST_0);
          mv.visitJumpInsn(Opcodes.GOTO, done);
          mv.visitLabel(isTrue);
          mv.visitInsn(Opcodes.ICONST_M1);
          mv.visitLabel(done);
          mv.visitInsn(Opcodes.IRETURN);
        }
      });
      source.build();
      Class clazz = booleanCoerce.getGeneratedClass();
      boolean result = false;
      for (int i = 0; i < 10000; i++) {
          result = (boolean)clazz.getMethod("invoke").invoke(new Object());
          System.out.println(result);
      }
      Assert.assertFalse(result);
    }
    */
    static class BooleanCoerce extends UnitGenerator {
        BooleanCoerce(String name, ASMClassSource environment) {
            super(name, environment);
        }
    }
    
}


