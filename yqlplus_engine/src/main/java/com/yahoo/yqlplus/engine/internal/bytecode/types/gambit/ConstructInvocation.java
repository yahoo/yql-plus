/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.compiler.ConstructorGenerator;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NotNullableTypeWidget;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Arrays;
import java.util.List;

public class ConstructInvocation extends BytecodeInvocable {
    public static GambitCreator.Invocable constructor(final TypeWidget owner, TypeWidget... argsWidgets) {
        return constructor(owner.getJVMType(), owner, argsWidgets);
    }

    public static GambitCreator.Invocable constructor(final TypeWidget owner, List<TypeWidget> argsWidgets) {
        return constructor(owner.getJVMType(), owner, argsWidgets);
    }


    public static GambitCreator.Invocable boundInvoke(final TypeWidget owner, BytecodeExpression... args) {
        return boundInvoke(owner.getJVMType(), owner, args);
    }

    public static GambitCreator.Invocable boundInvoke(final TypeWidget owner, final List<BytecodeExpression> args) {
        return boundInvoke(owner.getJVMType(), owner, args);
    }


    public static GambitCreator.Invocable constructor(Type targetType, final TypeWidget returnType, TypeWidget... argsWidgets) {
        return constructor(targetType, returnType, argsWidgets == null ? ImmutableList.<TypeWidget>of() : Arrays.asList(argsWidgets));
    }

    public static GambitCreator.Invocable constructor(Type targetType, final TypeWidget returnType, List<TypeWidget> argsWidgets) {
        return new ConstructInvocation(returnType, argsWidgets, targetType.getInternalName(), createDescriptor(BaseTypeAdapter.VOID, argsWidgets));
    }

    public static GambitCreator.Invocable boundInvoke(Type targetType, final TypeWidget owner, BytecodeExpression... args) {
        return boundInvoke(targetType, owner, args == null ? ImmutableList.<BytecodeExpression>of() : Arrays.asList(args));
    }

    public static GambitCreator.Invocable boundInvoke(Type targetType, final TypeWidget owner, final List<BytecodeExpression> args) {
        List<TypeWidget> types = Lists.newArrayListWithCapacity(args.size());
        for (BytecodeExpression expr : args) {
            types.add(expr.getType());
        }
        return constructor(targetType, owner, types).prefix(args);
    }

    public static GambitCreator.Invocable boundInvoke(Type targetType, final TypeWidget owner, final List<ConstructorGenerator> constructorGenerators, final BytecodeExpression... args) {
        if (null != constructorGenerators && !constructorGenerators.isEmpty()) {
            List<ConstructorGenerator> foundGenerators = Lists.newArrayList();
            for (ConstructorGenerator generator:constructorGenerators) {
                List<TypeWidget> argumentTypes = generator.getArgumentTypes();
                if (argumentTypes.size() != args.length) {
                    continue;
                }
                boolean assignable = true;
                for (int i = 0; i < argumentTypes.size(); i++) {
                    if (!argumentTypes.get(i).isAssignableFrom(args[i].getType())) {
                        assignable = false;
                    }
                    if (!assignable) {
                        break;
                    }
                }
                if (assignable) {
                    foundGenerators.add(generator);
                }
            }
            if (foundGenerators.size() == 1) {
                return constructor(targetType, owner, foundGenerators.get(0).getArgumentTypes()).prefix(args);
            } else if (foundGenerators.size() == 0) {
                throw new ProgramCompileException("No matching ConstructorGenerator");
            } else {
                throw new ProgramCompileException("Found ambiguous constructors for " + args);
            }          
        } else {
            throw new ProgramCompileException("No available ConstructorGenerator");
        }
    }
    
    private static String createDescriptor(TypeWidget returnType, List<TypeWidget> argsWidgets) {
        Type rt = returnType.getJVMType();
        List<Type> argTypes = Lists.newArrayListWithExpectedSize(argsWidgets.size());
        for (TypeWidget argsWidget : argsWidgets) {
            argTypes.add(argsWidget.getJVMType());
        }
        return Type.getMethodDescriptor(rt, argTypes.toArray(new Type[argTypes.size()]));
    }

    private final String ownerInternalName;
    private final String desc;

    private ConstructInvocation(TypeWidget returnType, List<TypeWidget> argsWidgets, String ownerInternalName, String desc) {
        super(NotNullableTypeWidget.create(returnType), argsWidgets);
        this.ownerInternalName = ownerInternalName;
        this.desc = desc;
    }

    @Override
    protected void generate(Location loc, CodeEmitter code, List<BytecodeExpression> args) {
        Preconditions.checkArgument(args.size() == getArgumentTypes().size(), "exactInvoker argument length mismatch: %s != expected %s", args.size(), getArgumentTypes().size());
        MethodVisitor mv = code.getMethodVisitor();
        List<BytecodeExpression> pre = Lists.newArrayListWithCapacity(args.size());
        CodeEmitter scope = code.createScope();
        for (BytecodeExpression arg : args) {
            pre.add(scope.evaluateOnce(arg));
        }
        mv.visitTypeInsn(Opcodes.NEW, ownerInternalName);
        mv.visitInsn(Opcodes.DUP);
        for (BytecodeExpression arg : pre) {
            arg.generate(scope);
        }
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, ownerInternalName, "<init>", desc, false);
        scope.endScope();
    }
}
