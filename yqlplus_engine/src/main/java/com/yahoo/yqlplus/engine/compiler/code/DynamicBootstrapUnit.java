/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.dynalang.dynalink.DynamicLinker;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public class DynamicBootstrapUnit extends UnitGenerator {
    public DynamicBootstrapUnit(ASMClassSource asmClassSource) {
        super(Dynamic.DYNAMIC_INTERNAL_NAME.replace('/', '.'), asmClassSource);
    }

    public void init() {
        EngineValueTypeAdapter types = environment.getValueTypeAdapter();
        BytecodeExpression classSource = types.constant(getEnvironment());
        TypeWidget dynamicLinker = types.adapt(DynamicLinker.class);
        FieldDefinition field = createField(dynamicLinker, "bootstrap");
        field.addModifier(Opcodes.ACC_STATIC | Opcodes.ACC_FINAL);
        MethodGenerator sl = createMethod("<clinit>");
        sl.setModifier(Opcodes.ACC_STATIC);
        sl.add(classSource);
        sl.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                final MethodVisitor mv = code.getMethodVisitor();
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(Dynamic.class),
                        "createDynamicLinker",
                        MethodType.methodType(DynamicLinker.class, ASMClassSource.class).toMethodDescriptorString(), false);
                mv.visitFieldInsn(Opcodes.PUTSTATIC,
                        Dynamic.DYNAMIC_INTERNAL_NAME,
                        "bootstrap",
                        Type.getDescriptor(DynamicLinker.class));
                mv.visitInsn(Opcodes.RETURN);

            }
        });

        MethodGenerator mgen = createStaticMethod(Dynamic.DYNAMIC_BOOTSTRAP_METHOD);
        TypeWidget callSite = types.adapt(CallSite.class);
        TypeWidget lookup = types.adapt(MethodHandles.Lookup.class);
        TypeWidget methodType = types.adapt(MethodType.class);
        mgen.setReturnType(callSite);
        AssignableValue lu = mgen.addArgument("lookup", lookup);
        AssignableValue name = mgen.addArgument("name", BaseTypeAdapter.STRING);
        AssignableValue sig = mgen.addArgument("sig", methodType);
        final String desc = MethodType.methodType(
                CallSite.class, // ...that will return a CallSite object, ...
                MethodHandles.Lookup.class, // ... when given a lookup object, ...
                String.class, // ... the operation name, ...
                MethodType.class, // ... and the signature at the call site.
                DynamicLinker.class
        ).toMethodDescriptorString();

        mgen.add(lu.read());
        mgen.add(name.read());
        mgen.add(sig.read());

        mgen.add(new BytecodeSequence() {
                     @Override
                     public void generate(CodeEmitter code) {
                         final MethodVisitor mv = code.getMethodVisitor();
                         mv.visitFieldInsn(Opcodes.GETSTATIC, Dynamic.DYNAMIC_INTERNAL_NAME, "bootstrap", Type.getDescriptor(DynamicLinker.class));
                         mv.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Dynamic.class), "link", desc, false);
                         mv.visitInsn(Opcodes.ARETURN);
                     }
                 }
        );

    }
}
