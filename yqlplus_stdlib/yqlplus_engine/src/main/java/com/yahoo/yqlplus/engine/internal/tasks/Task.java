/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.tasks;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * A task is something to do or something to wait for followed by zero or more tasks.
 */
public abstract class Task {
    private Set<Task> start = Sets.newIdentityHashSet();
    private String name;
    private Set<Value> available = ImmutableSet.of();

    public boolean addNext(Task task) {
        return start.add(task);
    }

    public int size() {
        return start.size();
    }

    public Iterable<Task> getNext() {
        return start;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract Set<Value> getInputs();

    public Set<Value> getAvailable() {
        return available;
    }

    public void setAvailable(Set<Value> available) {
        this.available = available;
    }
}
