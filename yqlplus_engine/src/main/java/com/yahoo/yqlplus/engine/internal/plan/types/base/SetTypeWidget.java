/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;


import com.yahoo.yqlplus.engine.internal.bytecode.IterableTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Type;

import java.util.HashSet;
import java.util.Set;

public class SetTypeWidget extends IterableTypeWidget {
    public SetTypeWidget(TypeWidget value) {
        super(Type.getType(Set.class), value);
    }

    public SetTypeWidget(Type type, TypeWidget value) {
        super(type, value);
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        return invokeNew(Type.getType(HashSet.class), arguments);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return true;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(final ProgramValueTypeAdapter typeAdapter) {
        return new UnificationAdapter() {
            @Override
            public TypeWidget unify(TypeWidget other) {
                // we *know* other matches our JVM type
                return new SetTypeWidget(typeAdapter.unifyTypes(SetTypeWidget.this.valueType, other.getIterableAdapter().getValue()));
            }
        };
    }

}
