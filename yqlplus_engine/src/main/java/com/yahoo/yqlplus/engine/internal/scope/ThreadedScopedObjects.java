/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.scope;

import com.yahoo.yqlplus.engine.scope.ExecutionThreadScope;

final class ThreadedScopedObjects extends ScopedObjects {
    private final ExecutionThreadScope scope;

    ThreadedScopedObjects(ExecutionThreadScope scope) {
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
