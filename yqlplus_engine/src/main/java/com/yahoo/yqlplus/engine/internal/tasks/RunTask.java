/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.tasks;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Execute a step and then start the next tasks.
 */
public final class RunTask extends Task {
    private List<Step> steps;

    public RunTask(List<Step> steps) {
        this.steps = steps;
    }

    public Iterable<Step> getSteps() {
        return steps;
    }

    @Override
    public Set<Value> getInputs() {
        // we need a list of the values that we need, but do not compute ourselves
        Set<Value> values = Sets.newIdentityHashSet();
        for (Task next : getNext()) {
            values.addAll(next.getInputs());
        }
        for (Step step : steps) {
            values.addAll(step.getInputs());
        }
        for (Step step : steps) {
            values.remove(step.getOutput());
        }
        return Sets.intersection(values, getAvailable());
    }
}
