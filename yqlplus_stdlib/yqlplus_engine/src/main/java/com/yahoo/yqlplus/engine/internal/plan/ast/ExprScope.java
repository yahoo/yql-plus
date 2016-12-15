/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.ast;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.Map;

public class ExprScope {
    private final Map<String, Boolean> locals = Maps.newLinkedHashMap();

    private int sym = 0;

    public int nextSuffix() {
        return ++sym;
    }

    public String gensym(String prefix) {
        return prefix + nextSuffix();
    }

    public String gensym() {
        return gensym("sym");
    }

    public void addArgument(String name) {
        locals.put(name, true);
    }

    public OperatorNode<FunctionOperator> createFunction(OperatorNode<PhysicalExprOperator> expr) {
        return OperatorNode.create(expr.getLocation(), FunctionOperator.FUNCTION, ImmutableList.copyOf(locals.keySet()), expr);
    }

}
