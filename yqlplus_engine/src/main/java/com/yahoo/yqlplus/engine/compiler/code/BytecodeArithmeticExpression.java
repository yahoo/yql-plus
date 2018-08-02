/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.compiler.runtime.ArithmeticOperation;
import com.yahoo.yqlplus.engine.compiler.runtime.Maths;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class BytecodeArithmeticExpression extends BaseTypeExpression {
    private final Location loc;
    private final ArithmeticOperation op;
    private BytecodeExpression leftExpr;
    private BytecodeExpression rightExpr;


    public BytecodeArithmeticExpression(Location loc, TypeWidget resultType, ArithmeticOperation op, BytecodeExpression leftExpr, BytecodeExpression rightExpr) {
        super(resultType);
        this.loc = loc;
        this.op = op;
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;

    }

    @Override
    public void generate(CodeEmitter code) {
        MethodVisitor mv = code.getMethodVisitor();
        Label isNull = new Label();
        TypeWidget mathType = getType().unboxed();

        if (!mathType.isPrimitive() || op == ArithmeticOperation.POW) {
            mv.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(Maths.class), "INSTANCE", Type.getDescriptor(Maths.class));
            mv.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(ArithmeticOperation.class), op.name(), Type.getDescriptor(ArithmeticOperation.class));
        }

        CodeEmitter.Unification out = code.unifyAs(mathType, leftExpr, rightExpr, isNull, isNull, isNull);
        // now we have both sides unified as (if possible) a primitive type

        // compute the result
        if (mathType.isPrimitive()) {
            switch (op) {
                case ADD:
                    mv.visitInsn(mathType.getJVMType().getOpcode(Opcodes.IADD));
                    break;
                case SUB:
                    mv.visitInsn(mathType.getJVMType().getOpcode(Opcodes.ISUB));
                    break;
                case MULT:
                    mv.visitInsn(mathType.getJVMType().getOpcode(Opcodes.IMUL));
                    break;
                case DIV:
                    mv.visitInsn(mathType.getJVMType().getOpcode(Opcodes.IDIV));
                    break;
                case MOD:
                    mv.visitInsn(mathType.getJVMType().getOpcode(Opcodes.IREM));
                    break;
                case POW:
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(Maths.class), "binaryMath",
                            Type.getMethodDescriptor(mathType.getJVMType(),
                                    Type.getType(ArithmeticOperation.class),
                                    mathType.getJVMType(),
                                    mathType.getJVMType()), false);
                    break;
                default:
                    throw new ProgramCompileException(loc, "Unknown BinaryMath operation: " + op);

            }
        } else if (mathType.getValueCoreType() == YQLCoreType.ANY) {
            String desc = Type.getMethodDescriptor(getType().getJVMType(),
                    Type.getType(Maths.class),
                    Type.getType(ArithmeticOperation.class),
                    leftExpr.getType().boxed().getJVMType(),
                    rightExpr.getType().boxed().getJVMType());
            mv.visitInvokeDynamicInsn("dyn:callMethod:binaryMath", desc, Dynamic.H_DYNALIB_BOOTSTRAP);
        } else {
            throw new ProgramCompileException(loc, "Math operation %s is not defined for type %s", op, mathType.getJVMType());
        }

        if (!getType().isPrimitive() && mathType.isPrimitive()) {
            code.box(mathType);
        }

        if (out.nullPossible) {
            Label done = new Label();
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(isNull);
            if (!mathType.isPrimitive() || op == ArithmeticOperation.POW) {
                mv.visitInsn(Opcodes.POP2);
            }
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitLabel(done);
        }
    }


}
