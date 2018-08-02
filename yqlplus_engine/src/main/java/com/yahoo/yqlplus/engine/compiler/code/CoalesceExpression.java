package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

class CoalesceExpression extends BaseTypeExpression {
    private final BytecodeExpression primaryValue;
    private final BytecodeExpression defaultValue;

    public CoalesceExpression(TypeWidget output, BytecodeExpression primaryValue, BytecodeExpression defaultValue) {
        super(output);
        this.primaryValue = primaryValue;
        this.defaultValue = defaultValue;
    }

    @Override
    public void generate(CodeEmitter code) {
        AssignableValue val = code.allocate(primaryValue);
        Label isNull = new Label();
        Label isDone = new Label();
        if (code.gotoIfNull(val, isNull)) {
            code.exec(val);
            code.cast(getType(), val.getType());
            MethodVisitor mv = code.getMethodVisitor();
            mv.visitJumpInsn(Opcodes.GOTO, isDone);
            mv.visitLabel(isNull);
            code.exec(defaultValue);
            code.cast(getType(), defaultValue.getType());
            mv.visitLabel(isDone);
        } else {
            code.exec(val);
            code.cast(getType(), val.getType());
        }
    }
}
