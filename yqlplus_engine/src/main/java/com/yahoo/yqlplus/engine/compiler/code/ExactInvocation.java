/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Arrays;
import java.util.List;

public class ExactInvocation extends BytecodeInvocable {

    public static GambitCreator.Invocable exactInvoke(int op, final String methodName, final TypeWidget owner, TypeWidget returnType, TypeWidget... argsWidgets) {
        return exactInvoke(op, methodName, owner, returnType, argsWidgets == null ? ImmutableList.of() : Arrays.asList(argsWidgets));
    }

    private static boolean hasReceiver(int op) {
        return op == Opcodes.INVOKEVIRTUAL || op == Opcodes.INVOKEINTERFACE;
    }

    public static GambitCreator.Invocable exactInvoke(int op, final String methodName, final TypeWidget owner, TypeWidget returnType, List<TypeWidget> argsWidgets) {
        final String desc = createDescriptor(returnType, argsWidgets);
        if (hasReceiver(op)) {
            argsWidgets = prefixTypes(owner, argsWidgets);
        }
        return new ExactInvocation(returnType, argsWidgets, op, owner.getJVMType().getInternalName(), methodName, desc);
    }

    private static String createDescriptor(TypeWidget returnType, List<TypeWidget> argsWidgets) {
        Type rt = returnType.getJVMType();
        List<Type> argTypes = Lists.newArrayListWithExpectedSize(argsWidgets.size());
        for (TypeWidget argsWidget : argsWidgets) {
            argTypes.add(argsWidget.getJVMType());
        }
        return Type.getMethodDescriptor(rt, argTypes.toArray(new Type[argTypes.size()]));
    }

    private static List<TypeWidget> prefixTypes(TypeWidget owner, List<TypeWidget> argsWidgets) {
        List<TypeWidget> fullArgsWidgets = Lists.newArrayListWithExpectedSize(argsWidgets.size() + 1);
        fullArgsWidgets.add(owner);
        fullArgsWidgets.addAll(argsWidgets);
        argsWidgets = fullArgsWidgets;
        return argsWidgets;
    }

    public static GambitCreator.Invocable boundInvoke(int op, final String methodName, final TypeWidget owner, TypeWidget returnType, BytecodeExpression... args) {
        return boundInvoke(op, methodName, owner, returnType, args == null ? ImmutableList.of() : Arrays.asList(args));
    }

    public static GambitCreator.Invocable boundInvoke(int op, final String methodName, final TypeWidget owner, TypeWidget returnType, final List<BytecodeExpression> args) {
        List<TypeWidget> types = Lists.newArrayListWithCapacity(args.size());
        for (BytecodeExpression expr : hasReceiver(op) ? args.subList(1, args.size()) : args) {
            types.add(expr.getType());
        }
        return exactInvoke(op, methodName, owner, returnType, types)
                .prefix(args);
    }

    private final int op;
    private final String ownerInternalName;
    private final String methodName;
    private final String desc;

    private ExactInvocation(TypeWidget returnType, List<TypeWidget> argsWidgets, int op, String ownerInternalName, String methodName, String desc) {
        super(returnType, argsWidgets);
        this.op = op;
        this.ownerInternalName = ownerInternalName;
        this.methodName = methodName;
        this.desc = desc;
    }


    @Override
    protected void generate(Location loc, CodeEmitter code, List<BytecodeExpression> args) {
        Preconditions.checkArgument(args.size() == getArgumentTypes().size(), "exactInvoker argument length mismatch: %s != expected %s", args.size(), getArgumentTypes().size());
        for (BytecodeExpression arg : args) {
            code.exec(arg);
        }
        code.getMethodVisitor().visitMethodInsn(op, ownerInternalName, methodName, desc, op == Opcodes.INVOKEINTERFACE);
    }
}
