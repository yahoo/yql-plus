/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.internal.operations.Maths;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.base.Dynamic;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class BytecodeNegateExpression extends BaseTypeExpression {
    private final Location loc;
    private BytecodeExpression leftExpr;


    public BytecodeNegateExpression(Location loc, BytecodeExpression leftExpr) {
        super(leftExpr.getType());
        this.loc = loc;
        this.leftExpr = leftExpr;

    }

    @Override
    public void generate(CodeEmitter code) {
        MethodVisitor mv = code.getMethodVisitor();
        Label isNull = new Label();
        TypeWidget mathType = getType().unboxed();

        if (!mathType.isPrimitive()) {
            mv.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(Maths.class), "INSTANCE", Type.getDescriptor(Maths.class));
        }

        boolean maybeNull = code.cast(mathType, leftExpr.getType(), isNull);
        // compute the result
        if (mathType.isPrimitive()) {
            mv.visitInsn(mathType.getJVMType().getOpcode(Opcodes.INEG));
        } else if (mathType.getValueCoreType() == YQLCoreType.ANY) {
            String desc = Type.getMethodDescriptor(getType().getJVMType(),
                    Type.getType(Maths.class),
                    leftExpr.getType().getJVMType());
            mv.visitInvokeDynamicInsn("dyn:callMethod:negate", desc, Dynamic.H_DYNALIB_BOOTSTRAP);
        } else {
            throw new ProgramCompileException(loc, "Math operation NEGATE is not defined for type %s", mathType.getJVMType());
        }

        if (!getType().isPrimitive() && mathType.isPrimitive()) {
            code.box(mathType);
        }

        if (maybeNull) {
            Label done = new Label();
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(isNull);
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitLabel(done);
        }
    }


}
