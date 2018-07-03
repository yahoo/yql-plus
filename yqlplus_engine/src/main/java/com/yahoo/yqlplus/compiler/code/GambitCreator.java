/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.compiler.runtime.ArithmeticOperation;
import com.yahoo.yqlplus.compiler.runtime.BinaryComparison;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;

public interface GambitCreator extends GambitTypes {

    BytecodeExpression first(Location location, BytecodeExpression inputExpr);

    BytecodeExpression length(Location location, BytecodeExpression inputExpr);

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
        void jump(BytecodeExpression test, BytecodeExpression result);

        BytecodeExpression complete(BytecodeExpression result);
    }

    interface CaseBuilder {
        void when(BytecodeExpression test, BytecodeExpression value);

        BytecodeExpression exit(BytecodeExpression defaultCase);
    }

    interface IfBuilder {
        ScopedBuilder when(BytecodeExpression test);
        ScopedBuilder elseIf();
        BytecodeSequence build();
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

    LoopBuilder loop(BytecodeExpression test, BytecodeExpression result);

    IterateBuilder iterate(BytecodeExpression iterable);

    CatchBuilder tryCatchFinally();

    CaseBuilder createSwitch(BytecodeExpression expr);

    CaseBuilder createCase();

    IfBuilder createIf();

    BytecodeExpression and(Location loc, BytecodeExpression... inputs);

    BytecodeExpression and(Location loc, List<BytecodeExpression> inputs);

    BytecodeExpression or(Location loc, BytecodeExpression... inputs);

    BytecodeExpression or(Location loc, List<BytecodeExpression> inputs);

    BytecodeExpression not(Location loc, BytecodeExpression input);

    BytecodeExpression bool(Location loc, BytecodeExpression input);

    BytecodeExpression isNull(Location location, BytecodeExpression input);

    BytecodeExpression coalesce(Location loc, BytecodeExpression... inputs);

    BytecodeExpression coalesce(Location loc, List<BytecodeExpression> inputs);

    BytecodeExpression guarded(BytecodeExpression target, BytecodeExpression ifTargetIsNotNull);

    BytecodeExpression guarded(BytecodeExpression target, BytecodeExpression ifTargetIsNotNull, BytecodeExpression ifTargetIsNull);

    BytecodeExpression list(TypeWidget elementType);

    BytecodeExpression list(Location loc, BytecodeExpression... args);

    BytecodeExpression list(Location loc, List<BytecodeExpression> args);

    BytecodeExpression newArray(Location loc, TypeWidget elementType, BytecodeExpression count);

    BytecodeExpression array(Location loc, TypeWidget elementType, BytecodeExpression... args);

    BytecodeExpression array(Location loc, TypeWidget elementType, List<BytecodeExpression> args);

    BytecodeExpression call(Location location, TypeWidget outputType, String name, List<BytecodeExpression> argumentExprs);

    BytecodeExpression invoke(Location loc, BytecodeExpression target, String operationName, BytecodeExpression... args);

    BytecodeExpression invoke(Location loc, BytecodeExpression target, String operationName, List<BytecodeExpression> args);

    BytecodeExpression invokeExact(Location loc, String methodName, Class<?> owner, TypeWidget returnType, BytecodeExpression... args);

    BytecodeExpression invokeExact(Location loc, String methodName, Class<?> owner, TypeWidget returnType, List<BytecodeExpression> args);

    BytecodeExpression invokeStatic(Location loc, String methodName, Class<?> owner, TypeWidget returnType, BytecodeExpression... args);

    BytecodeExpression invokeStatic(Location loc, String methodName, Class<?> owner, TypeWidget returnType, List<BytecodeExpression> args);

    Invocable constructor(TypeWidget type, TypeWidget... argumentTypes);

    Invocable constructor(TypeWidget type, List<TypeWidget> argumentTypes);

    BytecodeExpression invoke(Location loc, Invocable invocable, BytecodeExpression... args);

    BytecodeExpression invoke(Location loc, Invocable invocable, List<BytecodeExpression> args);

    BytecodeExpression negate(Location loc, BytecodeExpression input);

    BytecodeExpression arithmetic(Location loc, ArithmeticOperation op, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression contains(Location loc, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression matches(Location loc, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression in(Location loc, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression eq(Location loc, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression neq(Location loc, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression compare(Location loc, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression compare(Location loc, BinaryComparison op, BytecodeExpression left, BytecodeExpression right);

    BytecodeExpression composeCompare(List<BytecodeExpression> compares);

    BytecodeExpression transform(Location location, BytecodeExpression iterable, Invocable function);

    BytecodeExpression propertyValue(Location loc, BytecodeExpression target, String propertyName);

    AssignableValue propertyRef(Location loc, BytecodeExpression target, String propertyName);

    BytecodeExpression indexValue(Location loc, BytecodeExpression target, BytecodeExpression index);

    AssignableValue indexRef(Location loc, BytecodeExpression target, BytecodeExpression index);

    BytecodeExpression cast(Location loc, TypeWidget type, BytecodeExpression input);

    BytecodeExpression cast(TypeWidget type, BytecodeExpression input);

    BytecodeExpression fallback(Location loc, BytecodeExpression primary, BytecodeExpression caught);

    BytecodeExpression resolve(Location loc, BytecodeExpression timeout, BytecodeExpression promise);

    // Invocable should take one argument: a promise to be resolved (resolving will not block)
    BytecodeExpression resolveLater(Location loc, BytecodeExpression timeout, BytecodeExpression promise, Invocable callback);

    BytecodeExpression resolveLater(Location loc, BytecodeExpression timeout, BytecodeExpression promise, Invocable success, Invocable failure);
}
