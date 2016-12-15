/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.operator;

public class OperatorTreeVisitor implements OperatorNodeVisitor {
    @Override
    public void visit(Object arg) {

    }

    @Override
    public <T extends Operator> boolean enter(OperatorNode<T> node) {
        return true;
    }

    @Override
    public <T extends Operator> void exit(OperatorNode<T> node) {
    }
}
