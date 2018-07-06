/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;

public interface GambitCreator extends GambitTypes {

    interface Invocable {
        TypeWidget getReturnType();

        List<TypeWidget> getArgumentTypes();

        Invocable prefix(BytecodeExpression... arguments);

        Invocable prefix(List<BytecodeExpression> arguments);

        BytecodeExpression invoke(final Location loc, List<BytecodeExpression> args);

        BytecodeExpression invoke(final Location loc, BytecodeExpression... args);
    }

    interface RecordBuilder {
        RecordBuilder add(Location loc, String fieldName, BytecodeExpression input);

        RecordBuilder merge(Location loc, BytecodeExpression recordType);

        BytecodeExpression build();
    }

    interface ScopeBuilder extends ScopedBuilder {
        BytecodeExpression complete(BytecodeExpression result);
    }

    interface CaseBuilder {
        void when(BytecodeExpression test, BytecodeExpression value);

        BytecodeExpression exit(BytecodeExpression defaultCase);
    }

    interface CatchBuilder {
        ScopedBuilder body();

        ScopedBuilder on(String varName, TypeWidget exceptionType, TypeWidget... moreExceptionTypes);

        ScopedBuilder on(String varName, Class<?> exceptionType, Class<?>... moreExceptionTypes);

        ScopedBuilder always();

        BytecodeSequence build();
    }

    interface LoopBuilder extends ScopedBuilder {
        void abort(BytecodeExpression test);

        void next(BytecodeExpression test);

        BytecodeExpression build();
    }

    interface IterateBuilder extends ScopedBuilder {
        BytecodeExpression getItem();

        void abort(BytecodeExpression test);

        void next(BytecodeExpression test);

        BytecodeExpression build(BytecodeExpression result);

        BytecodeExpression build();
    }

    RecordBuilder record();
    RecordBuilder dynamicRecord();

    ScopeBuilder scope();

    IterateBuilder iterate(BytecodeExpression iterable);

    CatchBuilder tryCatchFinally();

    CaseBuilder createCase();

    BytecodeExpression not(Location loc, BytecodeExpression input);

    BytecodeExpression isNull(Location location, BytecodeExpression input);

    BytecodeExpression coalesce(Location loc, BytecodeExpression... inputs);

    BytecodeExpression coalesce(Location loc, List<BytecodeExpression> inputs);

    BytecodeExpression guarded(BytecodeExpression target, BytecodeExpression ifTargetIsNotNull);

    BytecodeExpression list(TypeWidget elementType);

    BytecodeExpression list(Location loc, List<BytecodeExpression> args);

    BytecodeExpression array(Location loc, TypeWidget elementType, List<BytecodeExpression> args);

    BytecodeExpression invokeExact(Location loc, String methodName, Class<?> owner, TypeWidget returnType, BytecodeExpression... args);

    Invocable constructor(TypeWidget type, TypeWidget... argumentTypes);

    Invocable constructor(TypeWidget type, List<TypeWidget> argumentTypes);

    BytecodeExpression transform(Location location, BytecodeExpression iterable, Invocable function);

    BytecodeExpression propertyValue(Location loc, BytecodeExpression target, String propertyName);

    BytecodeExpression indexValue(Location loc, BytecodeExpression target, BytecodeExpression index);

    BytecodeExpression cast(Location loc, TypeWidget type, BytecodeExpression input);

    BytecodeExpression cast(TypeWidget type, BytecodeExpression input);

    BytecodeExpression resolve(Location loc, BytecodeExpression timeout, BytecodeExpression promise);

}
