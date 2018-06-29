/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;


import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.IndexAdapter;
import com.yahoo.yqlplus.compiler.code.IterateAdapter;
import com.yahoo.yqlplus.compiler.code.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;

public final class ListTypeWidget extends IterableTypeWidget {
    public ListTypeWidget(TypeWidget value) {
        super(Type.getType(List.class), value);
    }

    public ListTypeWidget(Type type, TypeWidget value) {
        super(type, value);
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return new ListIndexAdapter(List.class, this, valueType);
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new ListIndexAdapter(List.class, this, valueType);
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        return invokeNew(Type.getType(ArrayList.class), arguments);
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
                return new ListTypeWidget(typeAdapter.unifyTypes(ListTypeWidget.this.valueType, other.getIterableAdapter().getValue()));
            }
        };
    }
}
