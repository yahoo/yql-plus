/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class CompareExpression implements BytecodeExpression {
    private final Location loc;
    private BytecodeExpression leftExpr;
    private BytecodeExpression rightExpr;

    public CompareExpression(Location loc, BytecodeExpression leftExpr, BytecodeExpression rightExpr) {
        this.loc = loc;
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }

    @Override
    public TypeWidget getType() {
        return BaseTypeAdapter.INT32;
    }


    @Override
    public void generate(CodeEmitter code) {
        // a bit of a hack; should not need to go to dynamic invocation for this unless one arg is ANY
        Label done = new Label();
        MethodVisitor mv = code.getMethodVisitor();
        Label leftNull = new Label();
        Label rightNull = new Label();
        Label bothNull = new Label();
        CodeEmitter.Unification unified = code.unifiedEmit(leftExpr, rightExpr, leftNull, rightNull, bothNull);
        if (unified.type.isPrimitive()) {
            emitPrimitiveCompare(code, unified.type);
        } else {
            // TODO: statically determine if the unified type is Comparable -- for now treat them all like "any"
            CodeEmitter scope = code.createScope();
            MethodVisitor mv2 = scope.getMethodVisitor();
            AssignableValue right = scope.allocate(unified.type);
            AssignableValue left = scope.allocate(unified.type);
            scope.exec(right.write(unified.type));
            scope.exec(left.write(unified.type));
            scope.exec(left.read());
            Label leftIsNotComparable = new Label();
            scope.emitInstanceCheck(unified.type, Comparable.class, leftIsNotComparable);
            scope.exec(right.read());
            mv2.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Comparable.class), "compareTo",
                    Type.getMethodDescriptor(Type.INT_TYPE, Type.getType(Object.class)), true);
            scope.gotoExitScope();
            mv2.visitLabel(leftIsNotComparable);
            scope.exec(left.read());
            scope.emitIntConstant((loc != null) ? loc.getLineNumber() : -1);
            scope.emitIntConstant((loc != null) ? loc.getCharacterOffset() : 0);
            mv2.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(YQLTypeException.class), "notComparable", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Object.class), Type.INT_TYPE, Type.INT_TYPE), false);
            // this bit is not reachable, notComparable throws
            mv2.visitInsn(Opcodes.ICONST_0);
            mv2.visitJumpInsn(Opcodes.GOTO, done);
            scope.endScope();
        }
        if (unified.nullPossible) {
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(leftNull);
            mv.visitInsn(Opcodes.ICONST_M1);
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(rightNull);
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(bothNull);
            mv.visitInsn(Opcodes.ICONST_0);
        }
        mv.visitLabel(done);
    }

    private void emitPrimitiveCompare(CodeEmitter code, TypeWidget type) {
        switch (type.getJVMType().getSort()) {
            case Type.BYTE:
            case Type.BOOLEAN:
            case Type.SHORT:
            case Type.INT:
            case Type.CHAR:
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Integer.class), "compare", Type.getMethodDescriptor(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE), false);
                break;
            case Type.FLOAT:
                code.getMethodVisitor().visitInsn(Opcodes.FCMPG);
                break;
            case Type.LONG:
                code.getMethodVisitor().visitInsn(Opcodes.LCMP);
                break;
            case Type.DOUBLE:
                code.getMethodVisitor().visitInsn(Opcodes.DCMPG);
                break;
            default:
                throw new UnsupportedOperationException("Unexpected primitive type: " + leftExpr.getType().getJVMType().getDescriptor());
        }
    }


}
