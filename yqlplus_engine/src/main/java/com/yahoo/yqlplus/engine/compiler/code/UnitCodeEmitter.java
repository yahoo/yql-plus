/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.yahoo.yqlplus.api.types.YQLBaseType;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;

public final class UnitCodeEmitter implements CodeEmitter {
    UnitGenerator unit;
    LocalFrame locals;
    MethodVisitor methodVisitor;
    final CodeEmitter parent;
    int sym;
    // TODO: keep track of if child emitters have had endScope called before permitting any further use of parent

    public UnitCodeEmitter(UnitGenerator unit, LocalFrame arguments, MethodVisitor methodVisitor) {
        this.unit = unit;
        this.locals = new LocalFrame(arguments);
        this.methodVisitor = methodVisitor;
        this.parent = null;
        startScope();
    }

    private UnitCodeEmitter(UnitCodeEmitter parent) {
        this.unit = parent.unit;
        this.locals = new LocalFrame(parent.locals);
        this.methodVisitor = parent.methodVisitor;
        this.parent = parent;
    }

    @Override
    public CodeEmitter createScope() {
        CodeEmitter out = new UnitCodeEmitter(this);
        out.startScope();
        return out;
    }

    @Override
    public CodeEmitter createScope(LocalFrame locals) {
        UnitCodeEmitter out = new UnitCodeEmitter(this);
        out.locals.replaceFrom(locals);
        out.startScope();
        return out;
    }

    @Override
    public void checkFrame(LocalValue localValue) {
        locals.check(localValue);
    }


    @Override
    public void startScope() {
        locals.startFrame(this);
    }

    @Override
    public void endScope() {
        locals.endFrame(this);
    }

    @Override
    public void emitIntConstant(int constant) {
        switch (constant) {
            case -1:
                methodVisitor.visitInsn(Opcodes.ICONST_M1);
                break;
            case 0:
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                break;
            case 1:
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                break;
            case 2:
                methodVisitor.visitInsn(Opcodes.ICONST_2);
                break;
            case 3:
                methodVisitor.visitInsn(Opcodes.ICONST_3);
                break;
            case 4:
                methodVisitor.visitInsn(Opcodes.ICONST_4);
                break;
            case 5:
                methodVisitor.visitInsn(Opcodes.ICONST_5);
                break;
            default:
                methodVisitor.visitLdcInsn(constant);
        }
    }

    @Override
    public void emitBooleanConstant(boolean constant) {
        if (constant) {
            methodVisitor.visitInsn(ICONST_1);
        } else {
            methodVisitor.visitInsn(ICONST_0);
        }
    }

    @Override
    public Label getStart() {
        return locals.getStartFrame();
    }

    @Override
    public Label getEnd() {
        return locals.getEndFrame();
    }

    @Override
    public void gotoExitScope() {
        methodVisitor.visitJumpInsn(Opcodes.GOTO, getEnd());
    }

    @Override
    public void inc(AssignableValue offsetExpr, int val) {
        Preconditions.checkArgument(offsetExpr.getType().getJVMType().getSort() == Type.INT, "inc applies only to INT types");
        if (offsetExpr instanceof LocalValue) {
            LocalValue localValue = (LocalValue) offsetExpr;
            methodVisitor.visitIincInsn(localValue.getStart(), val);
        } else {
            exec(offsetExpr.read());
            emitIntConstant(val);
            methodVisitor.visitInsn(Opcodes.IADD);
            exec(offsetExpr.write(offsetExpr.getType()));
        }
    }

    @Override
    public LocalValue getLocal(String name) {
        return locals.get(name);
    }

    @Override
    public LocalValue allocate(TypeWidget type, String name) {
        return locals.allocate(name, type);
    }

    @Override
    public BytecodeExpression evaluateOnce(BytecodeExpression target) {
        if (target instanceof EvaluatedExpression) {
            return target;
        }
        return allocate(target).read();
    }

    @Override
    public AssignableValue allocate(TypeWidget type) {
        return locals.allocate(gensym("local"), type);
    }


    @Override
    public AssignableValue evaluate(BytecodeExpression expr) {
        AssignableValue out = allocate(expr.getType());
        exec(out.write(expr));
        return out;
    }

    @Override
    public AssignableValue allocate(String name, TypeWidget type) {
        return locals.allocate(name, type);
    }

    @Override
    public AssignableValue evaluate(String name, BytecodeExpression expr) {
        AssignableValue out = allocate(name, expr.getType());
        exec(out.write(expr));
        return out;
    }

    @Override
    public void alias(String from, String to) {
        locals.alias(from, to);
    }

    @Override
    public MethodVisitor getMethodVisitor() {
        return methodVisitor;
    }


    @Override
    public void unbox(TypeWidget type) {
        cast(type.unboxed(), type);
    }

    @Override
    public void box(TypeWidget type) {
        cast(type.boxed(), NotNullableTypeWidget.create(type));
    }

    @Override
    public void emitStringConstant(String constant) {
        methodVisitor.visitLdcInsn(constant);
    }

    @Override
    public String gensym(String prefix) {
        return prefix + (++sym);
    }

    @Override
    public TypeWidget adapt(Class<?> clazz) {
        return unit.getEnvironment().getValueTypeAdapter().adapt(clazz);
    }

    @Override
    public TypeWidget adapt(java.lang.reflect.Type clazz) {
        return unit.getEnvironment().getValueTypeAdapter().adapt(clazz);
    }

    @Override
    public TypeWidget selectMax(TypeWidget left, TypeWidget right) {
        if (left.getValueCoreType().ordinal() > right.getValueCoreType().ordinal()) {
            return left;
        } else {
            return right;
        }
    }

    @Override
    public boolean isNumeric(TypeWidget type) {
        return isInteger(type) || isFloat(type);
    }

    @Override
    public boolean isInteger(TypeWidget type) {
        return YQLBaseType.INTEGERS.contains(type.getValueCoreType());
    }

    @Override
    public boolean isFloat(TypeWidget type) {
        return YQLBaseType.FLOATS.contains(type.getValueCoreType());
    }

    @Override
    public Unification unifiedEmit(BytecodeExpression left, BytecodeExpression right, Label leftNull, Label rightNull, Label bothNull) {
        // unify left and right (widening as needed); check for nulls and emit hasNull if either side is null
        // return the unified type that matches operands
        // result is a stack with both operands (L, R) and neither is null OR a jump to hasNull
        // primitive types are unboxed after their null check
        // if either side is any, they are unified to not-null ANY (boxed if needed)
        TypeWidget leftType = left.getType();
        TypeWidget rightType = right.getType();
        if ((isInteger(leftType) && isInteger(rightType))
                || (isFloat(leftType) && isFloat(rightType))) {
            return unifyAs(selectMax(leftType, rightType).unboxed(), left, right, leftNull, rightNull, bothNull);
        } else if (isNumeric(leftType) && isFloat(rightType)) {
            // fine, just coerce them both to FLOAT64
            return unifyAs(BaseTypeAdapter.FLOAT64, left, right, leftNull, rightNull, bothNull);
        } else if (isNumeric(rightType) && isFloat(leftType)) {
            // fine, just coerce them both to FLOAT64
            return unifyAs(BaseTypeAdapter.FLOAT64, left, right, leftNull, rightNull, bothNull);
        } else if (leftType.getValueCoreType() == YQLCoreType.BOOLEAN && rightType.getValueCoreType() == YQLCoreType.BOOLEAN) {
            // both boolean!
            return unifyAs(BaseTypeAdapter.BOOLEAN, left, right, leftNull, rightNull, bothNull);
        } else if (leftType.getJVMType().getDescriptor().equals(rightType.getJVMType().getDescriptor())) {
            return unifyAs(leftType, left, right, leftNull, rightNull, bothNull);
        } else {
            return unifyAs(AnyTypeWidget.getInstance(), left, right, leftNull, rightNull, bothNull);
        }
    }

    @Override
    public Unification unifyAs(TypeWidget typeWidget, BytecodeExpression left, BytecodeExpression right, Label leftNull, Label rightNull, Label bothNull) {
        left.generate(this);
        boolean distinct = (leftNull != bothNull) && right.getType().isNullable();
        Label leftIsNull = distinct ? new Label() : leftNull;
        boolean nullpossible = cast(typeWidget, left.getType(), leftIsNull);
        if (nullpossible && distinct) {
            Label skip = new Label();
            methodVisitor.visitJumpInsn(Opcodes.GOTO, skip);
            methodVisitor.visitLabel(leftIsNull);
            right.generate(this);
            methodVisitor.visitJumpInsn(Opcodes.IFNULL, bothNull);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, leftNull);
            methodVisitor.visitLabel(skip);
        }
        Label pop = new Label();
        right.generate(this);
        if (cast(typeWidget, right.getType(), pop)) {
            Label done = new Label();
            nullpossible = true;
            methodVisitor.visitJumpInsn(Opcodes.GOTO, done);
            methodVisitor.visitLabel(pop);
            // pop the left-value
            pop(typeWidget);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, rightNull);
            methodVisitor.visitLabel(done);
        }
        return new Unification(typeWidget, nullpossible);
    }

    @Override
    public BinaryCoercion binaryCoercion(BytecodeExpression left, Class<?> leftClazz, BytecodeExpression right, Class<?> rightClazz, Label leftNull, Label rightNull, Label bothNull) {
        TypeWidget leftTarget = adapt(leftClazz);
        TypeWidget rightTarget = adapt(rightClazz);
        left.generate(this);
        boolean distinct = (leftNull != bothNull) && right.getType().isNullable();
        Label leftIsNull = distinct ? new Label() : leftNull;
        boolean leftMaybeNull = cast(leftTarget, left.getType(), leftIsNull);
        if (leftMaybeNull && distinct) {
            Label skip = new Label();
            methodVisitor.visitJumpInsn(Opcodes.GOTO, skip);
            methodVisitor.visitLabel(leftIsNull);
            right.generate(this);
            methodVisitor.visitJumpInsn(Opcodes.IFNULL, bothNull);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, leftNull);
            methodVisitor.visitLabel(skip);
        }
        Label pop = new Label();
        right.generate(this);
        boolean rightMaybeNull = false;
        if (cast(rightTarget, right.getType(), pop)) {
            Label done = new Label();
            rightMaybeNull = true;
            methodVisitor.visitJumpInsn(Opcodes.GOTO, done);
            methodVisitor.visitLabel(pop);
            // pop the left-value
            pop(leftTarget);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, rightNull);
            methodVisitor.visitLabel(done);
        }
        return new BinaryCoercion(leftMaybeNull, rightMaybeNull);
    }

    @Override
    public AssignableValue allocate(Class<?> clazz) {
        return allocate(adapt(clazz));
    }

    @Override
    public void exec(BytecodeSequence sequence) {
        sequence.generate(this);
    }

    @Override
    public AssignableValue allocate(BytecodeExpression initializer) {
        AssignableValue val = allocate(initializer.getType());
        val.write(initializer).generate(this);
        return val;
    }

    @Override
    public void emitNewArray(TypeWidget elementType, BytecodeExpression e) {
        MethodVisitor mv = getMethodVisitor();
        exec(e);
        cast(BaseTypeAdapter.INT32, e.getType());
        switch (elementType.getJVMType().getSort()) {
            case Type.BYTE:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_BYTE);
                break;
            case Type.BOOLEAN:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_BOOLEAN);
                break;
            case Type.SHORT:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_SHORT);
                break;
            case Type.INT:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_INT);
                break;
            case Type.CHAR:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_CHAR);
                break;
            case Type.FLOAT:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_FLOAT);
                break;
            case Type.LONG:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_LONG);
                break;
            case Type.DOUBLE:
                mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_DOUBLE);
                break;
            case Type.OBJECT:
                mv.visitTypeInsn(Opcodes.ANEWARRAY, elementType.getJVMType().getInternalName());
                break;
            default:
                throw new UnsupportedOperationException("unknown sort for newArray" + elementType.getJVMType());
        }

    }

    @Override
    public Unification unifiedEmit(BytecodeExpression left, BytecodeExpression right, Label hasNull) {
        return unifiedEmit(left, right, hasNull, hasNull, hasNull);
    }

    @Override
    public BytecodeExpression nullChecked(BytecodeExpression expr, Label isNull) {
        if (expr.getType().isNullable()) {
            BytecodeExpression input = evaluateOnce(expr);
            gotoIfNull(input, isNull);
            return new NullCheckedEvaluatedExpression(input);
        }
        return expr;
    }

    @Override
    public boolean gotoIfNull(BytecodeExpression expr, Label isNull) {
        if (expr.getType().isNullable()) {
            exec(expr);
            methodVisitor.visitJumpInsn(Opcodes.IFNULL, isNull);
            return true;
        }
        return false;
    }

    @Override
    public boolean nullTest(TypeWidget typeWidget, Label isNull) {
        if (typeWidget.isNullable()) {
            Label done = new Label();
            dup(typeWidget);
            getMethodVisitor().visitJumpInsn(Opcodes.IFNONNULL, done);
            pop(typeWidget);
            getMethodVisitor().visitJumpInsn(Opcodes.GOTO, isNull);
            getMethodVisitor().visitLabel(done);
            return true;
        }
        return false;
    }

    @Override
    public boolean nullTestLeaveNull(TypeWidget typeWidget, Label isNull) {
        if (typeWidget.isNullable()) {
            Label done = new Label();
            dup(typeWidget);
            getMethodVisitor().visitJumpInsn(Opcodes.IFNONNULL, done);
            getMethodVisitor().visitJumpInsn(Opcodes.GOTO, isNull);
            getMethodVisitor().visitLabel(done);
            return true;
        }
        return false;
    }


    @Override
    public void notNullTest(TypeWidget typeWidget, Label isNotNull) {
        if (typeWidget.isNullable()) {
            dup(typeWidget);
            getMethodVisitor().visitJumpInsn(Opcodes.IFNONNULL, isNotNull);
            pop(typeWidget);
        } else {
            getMethodVisitor().visitJumpInsn(Opcodes.GOTO, isNotNull);
        }

    }

    @Override
    public void emitInstanceOf(TypeWidget targetType, Class<?> clazz, Label isNotInstance) {
        Preconditions.checkArgument(!clazz.isPrimitive());
        Preconditions.checkArgument(!targetType.isPrimitive());
        if (targetType.getJVMType().getDescriptor().equals(Type.getDescriptor(clazz))) {
            // trivial -- we already know this to be true!
            // we don't need to emit any code here
        } else {
            // naively emit the instanceof -- be nice to statically determine if it's even possible for targetType to be an instance of clazz
            // --- and also to know if we don't need a cast to do so
            dup(targetType);
            getMethodVisitor().visitTypeInsn(Opcodes.INSTANCEOF, Type.getInternalName(clazz));
            getMethodVisitor().visitJumpInsn(Opcodes.IFEQ, isNotInstance);
            getMethodVisitor().visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(clazz));
        }
    }

    // like emitInstanceOf but only leave the value of it IS
    @Override
    public void emitInstanceCheck(TypeWidget targetType, Class<?> clazz, Label isNotInstance) {
        Label is = new Label();
        Label not = new Label();
        nullTest(targetType, isNotInstance);
        emitInstanceOf(targetType, clazz, not);
        getMethodVisitor().visitJumpInsn(Opcodes.GOTO, is);
        getMethodVisitor().visitLabel(not);
        pop(targetType);
        getMethodVisitor().visitJumpInsn(Opcodes.GOTO, isNotInstance);
        getMethodVisitor().visitLabel(is);
    }

    @Override
    public void emitIntegerSwitch(Map<Integer, Label> labels, Label defaultCase) {
        // TODO: we should see if the labels are dense, and if so use TABLESWITCH
        MethodVisitor mv = getMethodVisitor();
        List<Integer> codes = Lists.newArrayList(labels.keySet());
        Collections.sort(codes);
        int[] k = new int[codes.size()];
        Label[] kl = new Label[codes.size()];
        for (int i = 0; i < k.length; ++i) {
            k[i] = codes.get(i);
            kl[i] = labels.get(i);
        }
        mv.visitLookupSwitchInsn(defaultCase, k, kl);
        mv.visitJumpInsn(GOTO, defaultCase);
    }

    @Override
    public void emitStringSwitch(Map<String, Label> labels, Label defaultCase, boolean caseInsensitive) {
        MethodVisitor mv = getMethodVisitor();
        if (caseInsensitive) {
            mv.visitFieldInsn(GETSTATIC, "java/util/Locale", "ENGLISH", "Ljava/util/Locale;");
            mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/String", "toUpperCase", "(Ljava/util/Locale;)Ljava/lang/String;", false);
        }
        mv.visitInsn(DUP);
        AssignableValue tmp = allocate(String.class);
        tmp.write(BaseTypeAdapter.STRING).generate(this);
        mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/String", "hashCode", "()I", false);
        Multimap<Integer, SwitchLabel> keys = ArrayListMultimap.create();
        for (Map.Entry<String, Label> e : labels.entrySet()) {
            String name = e.getKey();
            if (caseInsensitive) {
                name = name.toUpperCase(Locale.ENGLISH);
            }
            keys.put(name.hashCode(), new SwitchLabel(name, e.getValue()));
        }
        List<Integer> codes = Lists.newArrayList(keys.keySet());
        Collections.sort(codes);
        int[] k = new int[codes.size()];
        Label[] kl = new Label[codes.size()];
        for (int i = 0; i < k.length; ++i) {
            k[i] = codes.get(i);
            kl[i] = new Label();
        }
        mv.visitLookupSwitchInsn(defaultCase, k, kl);
        for (int i = 0; i < k.length; ++i) {
            mv.visitLabel(kl[i]);
            for (SwitchLabel switchLabel : keys.get(k[i])) {
                mv.visitLdcInsn(switchLabel.name);
                tmp.read().generate(this);
                mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/String", "equals", "(Ljava/lang/Object;)Z", false);
                mv.visitJumpInsn(IFNE, switchLabel.label);
            }
            mv.visitJumpInsn(GOTO, defaultCase);
        }
    }

    @Override
    public void cast(TypeWidget targetType, TypeWidget sourceType) {
        if (targetType.equals(sourceType)) {
            return;
        }
        if (targetType.getJVMType().getDescriptor().equals(sourceType.getJVMType().getDescriptor())) {
            return;
        }
        if (targetType.getJVMType().getDescriptor().equals(AnyTypeWidget.getInstance().getJVMType().getDescriptor())) {
            box(sourceType);
            return;
        }
        BytecodeSequence code = Conversions.getConversion(sourceType.getJVMType(), targetType.getJVMType());
        if (code != null) {
            code.generate(this);
        } else if (targetType.isPrimitive() && !sourceType.isPrimitive()) {
            Preconditions.checkState(!targetType.boxed().isPrimitive());
            cast(targetType.boxed(), sourceType);
            cast(targetType, targetType.boxed());
        } else if (targetType.isPrimitive() || sourceType.isPrimitive()) {
            throw new ProgramCompileException("Cannot cast " + targetType + " from " + sourceType);
        } else {
            // should check for castability
            getMethodVisitor().visitTypeInsn(Opcodes.CHECKCAST, targetType.getJVMType().getInternalName());
        }
    }

    @Override
    public boolean cast(TypeWidget targetType, TypeWidget sourceType, Label isNull) {
        if (targetType.equals(sourceType)) {
            return nullTest(sourceType, isNull);
        }
        if (targetType.getJVMType().getDescriptor().equals(sourceType.getJVMType().getDescriptor())) {
            return nullTest(sourceType, isNull);
        }
        if (targetType.getJVMType().getDescriptor().equals(AnyTypeWidget.getInstance().getJVMType().getDescriptor())) {
            if (sourceType.isPrimitive()) {
                box(sourceType);
                return false;
            } else {
                return nullTest(sourceType, isNull);
            }
        }
        BytecodeSequence code = Conversions.getConversion(sourceType.getJVMType(), targetType.getJVMType());
        if (code != null) {
            boolean r = false;
            if (sourceType.isNullable()) {
                r = nullTest(sourceType, isNull);
            }
            code.generate(this);
            return r;
        } else if (targetType.isPrimitive() && !sourceType.isPrimitive()) {
            boolean r = cast(targetType.boxed(), sourceType, isNull);
            unbox(NotNullableTypeWidget.create(targetType.boxed()));
            return r;
        } else if (targetType.isPrimitive() || sourceType.isPrimitive()) {
            throw new ProgramCompileException("Cannot cast " + targetType + " from " + sourceType);
        } else {
            boolean r = nullTest(sourceType, isNull); // should check for castability
            getMethodVisitor().visitTypeInsn(Opcodes.CHECKCAST, targetType.getJVMType().getInternalName());
            return r;
        }
    }

    @Override
    public void pop(TypeWidget typeWidget) {
        switch (typeWidget.getJVMType().getSize()) {
            case 0:
                return;
            case 1:
                getMethodVisitor().visitInsn(Opcodes.POP);
                return;
            case 2:
                getMethodVisitor().visitInsn(Opcodes.POP2);
                return;
            default:
                throw new UnsupportedOperationException("Unexpected JVM type width: " + typeWidget.getJVMType());
        }
    }

    @Override
    public void dup(TypeWidget typeWidget) {
        switch (typeWidget.getJVMType().getSize()) {
            case 0:
                throw new UnsupportedOperationException();
            case 1:
                getMethodVisitor().visitInsn(Opcodes.DUP);
                return;
            case 2:
                getMethodVisitor().visitInsn(Opcodes.DUP2);
                return;
            default:
                throw new UnsupportedOperationException("Unexpected JVM type width: " + typeWidget.getJVMType());
        }
    }

    @Override
    public void swap(TypeWidget top, TypeWidget previous) {
        MethodVisitor mv = getMethodVisitor();
        switch (top.getJVMType().getSize()) {
            case 1:
                switch (previous.getJVMType().getSize()) {
                    case 1:
                        // 1, 1
                        mv.visitInsn(Opcodes.SWAP);
                        return;
                    case 2:
                        // 2, 1
                        // {value3, value2}, value1 → value1, {value3, value2}, value1
                        mv.visitInsn(Opcodes.DUP_X2);
                        // value1, {value3, value2}
                        mv.visitInsn(Opcodes.POP);
                        return;
                }
                break;
            case 2:
                switch (previous.getJVMType().getSize()) {
                    case 1:
                        // 1, 2
                        // value3, {value2, value1} → {value2, value1}, value3, {value2, value1}
                        mv.visitInsn(Opcodes.DUP2_X1);
                        // {value2, value1}, value3
                        mv.visitInsn(Opcodes.POP2);
                        return;
                    case 2:
                        // {value4, value3}, {value2, value1} → {value2, value1}, {value4, value3}, {value2, value1}
                        mv.visitInsn(Opcodes.DUP2_X2);
                        // {value2, value1}, {value4, value3}
                        mv.visitInsn(Opcodes.POP2);
                        return;
                }
                break;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void emitThrow(Class<? extends Throwable> exceptionType, BytecodeExpression... constructorArguments) {
        emitNew(Type.getInternalName(exceptionType), constructorArguments);
        methodVisitor.visitInsn(Opcodes.ATHROW);
    }

    @Override
    public void emitNew(String typeInternalName, BytecodeExpression... constructorArguments) {
        methodVisitor.visitTypeInsn(Opcodes.NEW, typeInternalName);
        methodVisitor.visitInsn(Opcodes.DUP);
        String desc = Type.getMethodDescriptor(Type.VOID_TYPE);
        if (constructorArguments != null && constructorArguments.length > 0) {
            Type[] argumentTypes = new Type[constructorArguments.length];
            for (int i = 0; i < constructorArguments.length; ++i) {
                argumentTypes[i] = constructorArguments[i].getType().getJVMType();
            }
            desc = Type.getMethodDescriptor(Type.VOID_TYPE, argumentTypes);
        }
        methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, typeInternalName, "<init>", desc, false);
    }

}
