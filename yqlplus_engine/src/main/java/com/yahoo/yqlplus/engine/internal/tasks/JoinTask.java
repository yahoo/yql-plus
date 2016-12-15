/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.tasks;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Wait for the prior tasks to complete and then start the next tasks. A JoinTask with no further tasks is used to make a 'done' node.
 */
public class JoinTask extends Task {
    private Set<Task> priors = Sets.newIdentityHashSet();

    public JoinTask() {
    }

    public Set<Task> getPriors() {
        return priors;
    }

    @Override
    public Set<Value> getInputs() {
        Set<Value> values = Sets.newIdentityHashSet();
        for (Task next : getNext()) {
            values.addAll(next.getInputs());
        }
        return Sets.intersection(values, getAvailable());
    }
}
