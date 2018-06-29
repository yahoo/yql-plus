package com.yahoo.yqlplus.compiler.code;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import java.util.Map;

public interface CodeEmitter extends VariableEnvironment {
    CodeEmitter createScope();

    CodeEmitter createScope(LocalFrame locals);

    void checkFrame(LocalValue localValue);

    void startScope();

    void endScope();

    void emitIntConstant(int constant);

    void emitBooleanConstant(boolean constant);

    Label getStart();

    Label getEnd();

    void gotoExitScope();

    void inc(AssignableValue offsetExpr, int val);

    LocalValue getLocal(String name);

    LocalValue allocate(TypeWidget type, String name);

    BytecodeExpression evaluateOnce(BytecodeExpression target);

    @Override
    AssignableValue allocate(TypeWidget type);

    @Override
    AssignableValue evaluate(BytecodeExpression expr);

    @Override
    AssignableValue allocate(String name, TypeWidget type);

    @Override
    AssignableValue evaluate(String name, BytecodeExpression expr);

    @Override
    void alias(String from, String to);

    MethodVisitor getMethodVisitor();

    void unbox(TypeWidget type);

    void box(TypeWidget type);

    void emitStringConstant(String constant);

    String gensym(String prefix);

    TypeWidget adapt(Class<?> clazz);

    TypeWidget adapt(java.lang.reflect.Type clazz);

    TypeWidget selectMax(TypeWidget left, TypeWidget right);

    boolean isNumeric(TypeWidget type);

    boolean isInteger(TypeWidget type);

    boolean isFloat(TypeWidget type);

    Unification unifiedEmit(BytecodeExpression left, BytecodeExpression right, Label leftNull, Label rightNull, Label bothNull);

    Unification unifyAs(TypeWidget typeWidget, BytecodeExpression left, BytecodeExpression right, Label leftNull, Label rightNull, Label bothNull);

    BinaryCoercion binaryCoercion(BytecodeExpression left, Class<?> leftClazz, BytecodeExpression right, Class<?> rightClazz, Label leftNull, Label rightNull, Label bothNull);

    AssignableValue allocate(Class<?> clazz);

    void exec(BytecodeSequence sequence);

    AssignableValue allocate(BytecodeExpression initializer);

    void emitNewArray(TypeWidget elementType, BytecodeExpression e);

    Unification unifiedEmit(BytecodeExpression left, BytecodeExpression right, Label hasNull);

    BytecodeExpression nullChecked(BytecodeExpression expr, Label isNull);

    boolean gotoIfNull(BytecodeExpression expr, Label isNull);

    boolean nullTest(TypeWidget typeWidget, Label isNull);

    boolean nullTestLeaveNull(TypeWidget typeWidget, Label isNull);

    void notNullTest(TypeWidget typeWidget, Label isNotNull);

    void emitInstanceOf(TypeWidget targetType, Class<?> clazz, Label isNotInstance);

    // like emitInstanceOf but only leave the value of it IS
    void emitInstanceCheck(TypeWidget targetType, Class<?> clazz, Label isNotInstance);

    void emitIntegerSwitch(Map<Integer, Label> labels, Label defaultCase);

    void emitStringSwitch(Map<String, Label> labels, Label defaultCase, boolean caseInsensitive);

    void cast(TypeWidget targetType, TypeWidget sourceType);

    boolean cast(TypeWidget targetType, TypeWidget sourceType, Label isNull);

    void pop(TypeWidget typeWidget);

    void dup(TypeWidget typeWidget);

    void swap(TypeWidget top, TypeWidget previous);

    void emitThrow(Class<? extends Throwable> exceptionType, BytecodeExpression... constructorArguments);

    void emitNew(String typeInternalName, BytecodeExpression... constructorArguments);

    class Unification {
        public final TypeWidget type;
        public final boolean nullPossible;

        public Unification(TypeWidget type, boolean nullPossible) {
            this.type = type;
            this.nullPossible = nullPossible;
        }
    }

    class BinaryCoercion {
        public final boolean leftNullable;
        public final boolean rightNullable;

        public BinaryCoercion(boolean leftNullable, boolean rightNullable) {
            this.leftNullable = leftNullable;
            this.rightNullable = rightNullable;
        }
    }

    class SwitchLabel {
        public final String name;
        public final Label label;

        public SwitchLabel(String name, Label label) {
            this.name = name;
            this.label = label;
        }
    }
}
