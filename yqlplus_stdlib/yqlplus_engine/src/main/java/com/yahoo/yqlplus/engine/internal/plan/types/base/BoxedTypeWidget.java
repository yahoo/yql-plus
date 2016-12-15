/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;

public class BoxedTypeWidget extends BaseTypeWidget {
    private final YQLCoreType coreType;
    private final TypeWidget unboxed;

    public BoxedTypeWidget(YQLCoreType coreType, Type type, TypeWidget unboxed) {
        super(type);
        this.coreType = coreType;
        this.unboxed = unboxed;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return coreType;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public TypeWidget boxed() {
        return this;
    }

    @Override
    public TypeWidget unboxed() {
        return unboxed;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return new ComparisonAdapter() {
            @Override
            public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
                scope.unbox(BoxedTypeWidget.this);
                unboxed.getComparisionAdapter().coerceBoolean(scope, isTrue, isFalse, isNull);
            }
        };
    }

    @Override
    public SerializationAdapter getSerializationAdapter(final NativeEncoding encoding) {
        return new SerializationAdapter() {
            @Override
            public BytecodeSequence serializeTo(final BytecodeExpression source, final BytecodeExpression generator) {
                return new BytecodeSequence() {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.exec(unboxed.getSerializationAdapter(encoding).serializeTo(new BytecodeCastExpression(unboxed, source), generator));
                    }
                };
            }

            @Override
            public BytecodeExpression deserializeFrom(final BytecodeExpression parser) {
                return new BytecodeCastExpression(BoxedTypeWidget.this,
                        unboxed.getSerializationAdapter(encoding).deserializeFrom(parser));
            }
        };
    }

    @Override
    protected SerializationAdapter getJsonSerializationAdapter() {
        // we override getSerializationAdapter(NativeEncoding)
        throw new UnsupportedOperationException();
    }
}
