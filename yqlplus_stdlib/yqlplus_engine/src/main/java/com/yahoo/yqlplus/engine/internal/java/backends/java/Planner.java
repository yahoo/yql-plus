/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.backends.java;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.lang.reflect.Method;
import java.util.List;

abstract class Planner implements QueryMethod {
    protected final String name;

    // if the method returns a singleton (which needs to be null checked and wrapped in an iterable)
    protected final boolean single;
    // if true this planner represents a function which return a ListenableFuture<Iterable<>> rather than Iterable<>
    protected final boolean async;
    protected final Method method;
    protected final TypeWidget returnType;
    protected final int timeoutArgument;
    int priority;
    long minimumBudget = -1;
    long maximumBudget = -1;

    Planner(String name, boolean single, boolean async, Method method, int timeoutArgument, TypeWidget returnType) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(method);
        Preconditions.checkNotNull(returnType);
        this.name = name;
        this.single = single;
        this.async = async;
        this.method = method;
        this.timeoutArgument = timeoutArgument;
        this.returnType = returnType;
    }

    public abstract PlannerMatch match(OperatorNode<ExpressionOperator> filter, List<OperatorNode<ExpressionOperator>> scanArguments);

    public void setBudget(long minimumBudget, long maximumBudget) {
        this.minimumBudget = minimumBudget;
        this.maximumBudget = maximumBudget;
    }


    @Override
    public TypeWidget getRecordType() {
        return returnType;
    }

    @Override
    public String getKeyName() {
        return null;
    }

    @Override
    public TypeWidget getKeyType() {
        return null;
    }
}
