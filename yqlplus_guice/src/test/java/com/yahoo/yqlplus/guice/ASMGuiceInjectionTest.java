package com.yahoo.yqlplus.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.yahoo.yqlplus.engine.compiler.code.*;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Modifier;

@Test
public class ASMGuiceInjectionTest {
    public interface Computor {
        int compute();
    }

    @Test
    public void requireInjector() throws Exception {
        final ASMClassSource source = new ASMClassSource();
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
        final ASMClassSource source = new ASMClassSource();
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
}
