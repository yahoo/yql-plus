/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

abstract class BytecodeInvocable implements GambitCreator.Invocable {
    private final TypeWidget returnType;
    private final List<TypeWidget> argTypes;

    BytecodeInvocable(TypeWidget returnType, List<TypeWidget> argTypes) {
        this.returnType = returnType;
        this.argTypes = ImmutableList.copyOf(argTypes);
    }

    @Override
    public TypeWidget getReturnType() {
        return returnType;
    }

    @Override
    public List<TypeWidget> getArgumentTypes() {
        return argTypes;
    }

    @Override
    public GambitCreator.Invocable prefix(BytecodeExpression... arguments) {
        if (arguments == null || arguments.length == 0) {
            return this;
        }
        return prefix(Arrays.asList(arguments));
    }

    @Override
    public GambitCreator.Invocable prefix(List<BytecodeExpression> arguments) {
        if (arguments.isEmpty()) {
            return this;
        }
        Preconditions.checkState(arguments.size() <= argTypes.size(), "Invocable passed too many arguments to prefix");
        List<BytecodeExpression> prefixed = Lists.newArrayListWithExpectedSize(arguments.size());
        prefixed.addAll(arguments);
        return new PrefixedBytecodeInvocable(returnType, prefixed, argTypes.subList(prefixed.size(), argTypes.size()), this);
    }

    public final BytecodeExpression invoke(Location loc, BytecodeExpression... args) {
        return invoke(loc, args == null ? ImmutableList.of() : Arrays.asList(args));
    }

    @Override
    public final BytecodeExpression invoke(final Location loc, List<BytecodeExpression> args) {
        final List<BytecodeExpression> argsCopy = Lists.newArrayListWithExpectedSize(args.size());
        Iterator<TypeWidget> argumentTypes = getArgumentTypes().iterator();
        for (BytecodeExpression arg : args) {
            Preconditions.checkNotNull(arg);
            argsCopy.add(new BytecodeCastExpression(argumentTypes.next(), arg));
        }
        return new BaseTypeExpression(returnType) {
            @Override
            public void generate(CodeEmitter code) {
                BytecodeInvocable.this.generate(loc, code, argsCopy);
            }
        };
    }

    protected abstract void generate(Location loc, CodeEmitter code, List<BytecodeExpression> args);

    private static class PrefixedBytecodeInvocable extends BytecodeInvocable {
        private final List<BytecodeExpression> prefix;
        private final BytecodeInvocable parent;

        PrefixedBytecodeInvocable(TypeWidget returnType, List<BytecodeExpression> prefixed, List<TypeWidget> argTypes, BytecodeInvocable parent) {
            super(returnType, argTypes);
            this.prefix = prefixed;
            this.parent = parent;
        }

        @Override
        public GambitCreator.Invocable prefix(List<BytecodeExpression> arguments) {
            if (arguments.isEmpty()) {
                return this;
            }
            Preconditions.checkState(arguments.size() <= getArgumentTypes().size(), "Invocable passed too many arguments to prefix");
            List<BytecodeExpression> prefixed = Lists.newArrayListWithExpectedSize(arguments.size() + prefix.size());
            prefixed.addAll(prefix);
            prefixed.addAll(arguments);
            return new PrefixedBytecodeInvocable(getReturnType(), prefixed, getArgumentTypes().subList(arguments.size(), getArgumentTypes().size()), parent);
        }

        @Override
        protected void generate(Location loc, CodeEmitter code, List<BytecodeExpression> args) {
            parent.generate(loc, code, Lists.newArrayList(Iterables.concat(prefix, args)));
        }
    }
}
