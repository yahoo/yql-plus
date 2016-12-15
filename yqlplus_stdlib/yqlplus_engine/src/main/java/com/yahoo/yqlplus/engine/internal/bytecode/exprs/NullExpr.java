/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.exprs;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.AnyTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NullableTypeWidget;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class NullExpr extends BaseTypeExpression {
    public NullExpr(TypeWidget type) {
        super(NullableTypeWidget.create(type));
    }

    @Override
    public void generate(CodeEmitter code) {
        // VOID, BOOLEAN, CHAR, BYTE, SHORT, INT, FLOAT, LONG, DOUBLE, ARRAY, OBJECT or METHOD.
        int opcode;
        switch (getType().getJVMType().getSort()) {
            case Type.VOID:
            case Type.METHOD:
                throw new UnsupportedOperationException("Unsupported NullExpr type: " + getType());
            case Type.BOOLEAN:
            case Type.SHORT:
            case Type.INT:
            case Type.CHAR:
                opcode = Opcodes.ICONST_0;
                break;
            case Type.FLOAT:
                opcode = Opcodes.FCONST_0;
                break;
            case Type.LONG:
                opcode = Opcodes.LCONST_0;
                break;
            case Type.DOUBLE:
                opcode = Opcodes.DCONST_0;
                break;
            case Type.ARRAY:
            case Type.OBJECT:
                opcode = Opcodes.ACONST_NULL;
                break;
            default:
                throw new UnsupportedOperationException("Unknown NullExpr type: " + getType());
        }
        code.getMethodVisitor().visitInsn(opcode);
        if (opcode == Opcodes.ACONST_NULL) {
            code.cast(getType(), AnyTypeWidget.getInstance());
        }
    }
}
