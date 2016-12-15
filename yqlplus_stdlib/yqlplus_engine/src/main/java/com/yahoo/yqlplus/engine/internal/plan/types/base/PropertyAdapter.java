/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;

import java.util.Map;

public interface PropertyAdapter {
    BytecodeExpression construct(Map<String, BytecodeExpression> fields);

    boolean isClosed();

    TypeWidget getPropertyType(String propertyName) throws PropertyNotFoundException;

    Iterable<Property> getProperties();

    AssignableValue property(BytecodeExpression target, String propertyName);

    AssignableValue index(BytecodeExpression target, BytecodeExpression propertyName);

    BytecodeSequence mergeIntoFieldWriter(BytecodeExpression target, BytecodeExpression fieldWriter);

    public interface PropertyVisit {
        void item(CodeEmitter code, BytecodeExpression propertyName, BytecodeExpression propertyValue, Label abortLoop, Label nextItem);
    }

    BytecodeSequence visitProperties(BytecodeExpression target, PropertyVisit loop);

    BytecodeExpression getPropertyNameIterable(BytecodeExpression target);

    final class Property {
        public final String name;
        public final TypeWidget type;

        public Property(String name, TypeWidget type) {
            this.name = name;
            this.type = type;
        }
    }
}
