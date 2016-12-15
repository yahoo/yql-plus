/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Key;
import com.google.inject.OutOfScopeException;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Singleton;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;
import com.yahoo.yqlplus.engine.scope.ExecutionThreadScope;

import java.util.Collections;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkState;

@Singleton
public class ExecutionScoper implements Scope {
    private final ThreadLocal<Stack<ScopedObjects>> scopeLocal = new ThreadLocal<>();

//    public static class ScopedObjects {
//        private final ExecutionScope scope;
//        private final Map<Key<?>, Object> scopedObjects = Maps.newHashMap();
//
//        private ScopedObjects(ExecutionScope scope) {
//            this.scope = scope;
//        }
//
//        public Map<Key<?>, Object> getScopedObjects() {
//            return Collections.unmodifiableMap(scopedObjects);
//        }
//
//        synchronized <T> T get(Key<?> key, Provider<T> unscoped) {
//            @SuppressWarnings("unchecked")
//            T current = (T) scope.get(key);
//            if (current != null) {
//                return current;
//            }
//
//            @SuppressWarnings("unchecked")
//            T target = (T) scopedObjects.get(key);
//            if (target == null && !scopedObjects.containsKey(key)) {
//                target = unscoped.get();
//                scopedObjects.put(key, target);
//            }
//            return target;
//        }
//
//        void exit() {
//
//        }
//
//        void enter() {
//
//        }
//    }

    static final class ThreadedScopedObjects extends ScopedObjects {
        private final ExecutionThreadScope scope;

        private ThreadedScopedObjects(ExecutionThreadScope scope) {
            super(scope);
            this.scope = scope;
        }

        @Override
        void exit() {
            scope.exit();
            super.exit();
        }

        @Override
        void enter() {
            super.enter();
            scope.enter();
        }
    }

    public Runnable startScope(final Runnable target, final ExecutionScope executionScope) {
        return scope(target, create(executionScope));
    }

    public <T> Callable<T> startScope(final Callable<T> target, final ExecutionScope executionScope) {
        return scope(target, create(executionScope));
    }

    private ScopedObjects create(ExecutionScope executionScope) {
        if (executionScope instanceof ExecutionThreadScope) {
            return new ThreadedScopedObjects((ExecutionThreadScope) executionScope);
        }
        return new ScopedObjects(executionScope);
    }


    private Executor scope(final Executor target, final ScopedObjects scope) {
        return new Executor() {
            @Override
            public void execute(Runnable command) {
                enter(scope);
                try {
                    target.execute(command);
                } finally {
                    exit();
                }
            }
        };
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

    private <T> FutureCallback<T> scope(final FutureCallback<T> callback, final ScopedObjects scope) {
        return new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                enter(scope);
                try {
                    callback.onSuccess(result);
                } finally {
                    exit();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                enter(scope);
                try {
                    callback.onFailure(t);
                } finally {
                    exit();
                }
            }
        };
    }


    public Runnable continueScope(final Runnable target) {
        return scope(target, getScope());
    }

    public <T> Callable<T> continueScope(final Callable<T> target) {
        return scope(target, getScope());
    }

    public <T> FutureCallback<T> continueScope(final FutureCallback<T> callback) {
        return scope(callback, getScope());
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

    public <T> ListenableFuture<T> scopeCallbacks(final ListenableFuture<T> callback) {
        final ScopedObjects scope = getScope();
        return new ForwardingListenableFuture<T>() {
            @Override
            protected ListenableFuture<T> delegate() {
                return callback;
            }

            @Override
            public void addListener(Runnable listener, Executor exec) {
                super.addListener(listener, scope(exec, scope));
            }
        };
    }
}
