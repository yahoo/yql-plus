/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.scope;

/**
 * An ExecutionScope which also requires .enter() and .exit() to be called on all participating threads.
 */
public interface ExecutionThreadScope extends ExecutionScope {
    void enter();

    void exit();
}
