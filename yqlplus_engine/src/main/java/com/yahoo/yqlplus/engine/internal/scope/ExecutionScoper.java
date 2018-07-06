/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.google.inject.Key;
import com.google.inject.OutOfScopeException;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Singleton;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;

import java.util.Stack;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkState;

@Singleton
public class ExecutionScoper implements Scope {
    private final ThreadLocal<Stack<ScopedObjects>> scopeLocal = new ThreadLocal<>();
    public <T> Callable<T> startScope(final Callable<T> target, final ExecutionScope executionScope) {
        return scope(target, create(executionScope));
    }

    private ScopedObjects create(ExecutionScope executionScope) {
        return new ScopedObjects(executionScope);
    }


    private Runnable scope(final Runnable target, final ScopedObjects scope) {
        return new Runnable() {
            @Override
            public void run() {
                enter(scope);
                try {
                    target.run();
                } finally {
                    exit();
                }
            }
        };
    }

    private <T> Callable<T> scope(final Callable<T> target, final ScopedObjects scope) {
        return new Callable<T>() {
            @Override
            public T call() throws Exception {
                enter(scope);
                try {
                    return target.call();
                } finally {
                    exit();
                }
            }
        };
    }


    public Runnable continueScope(final Runnable target) {
        return scope(target, getScope());
    }

    public ScopedObjects getScope() {
        Stack<ScopedObjects> scope = scopeLocal.get();
        if (scope == null) {
            throw new OutOfScopeException("Not inside an ExecutionScope");
        }
        return scope.peek();
    }

    public void enter(ScopedObjects scope) {
        Stack<ScopedObjects> scopes = scopeLocal.get();
        if (scopes == null) {
            scopes = new Stack<>();
            scopeLocal.set(scopes);
        }
        scope.enter();
        scopes.push(scope);
    }

    public void exit() {
        checkState(scopeLocal.get() != null, "No scoping block in progress");
        Stack<ScopedObjects> scopes = scopeLocal.get();
        ScopedObjects scope = scopes.pop();
        scope.exit();
        if (scopes.isEmpty()) {
            scopeLocal.remove();
        }
    }

    public Provider<ExecutionScope> getExecutionScope() {
        return new Provider<ExecutionScope>() {
            public ExecutionScope get() {
                return getScope().scope;
            }
        };
    }


    public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
        return new Provider<T>() {
            public T get() {
                return getScope().get(key, unscoped);
            }
        };
    }

}
