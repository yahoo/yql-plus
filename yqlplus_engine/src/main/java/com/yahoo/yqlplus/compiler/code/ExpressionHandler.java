/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.compiler.runtime.MapFieldWriter;
import com.yahoo.yqlplus.compiler.runtime.RecordMapWrapper;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class ExpressionHandler extends TypesHandler implements ScopedBuilder {
    protected LocalCodeChunk body;

    protected ExpressionHandler(ASMClassSource source) {
        super(source);
    }

    Label getStart() {
        return body.getStart();
    }

    Label getEnd() {
        return body.getEnd();
    }

    public RecordBuilder record() {
        return new ExpressionRecordBuilder();
    }

    public RecordBuilder dynamicRecord() {
        return new DynamicExpressionRecordBuilder();
    }

    AssignableValue evaluate(AssignableValue local, BytecodeExpression expr) {
        body.add(local.write(expr));
        return local;
    }

    @Override
    public AssignableValue allocate(TypeWidget type) {
        return body.allocate(type);
    }

    @Override
    public AssignableValue allocate(String name, TypeWidget type) {
        return body.allocate(name, type);
    }

    @Override
    public BytecodeExpression evaluateInto(String name, BytecodeExpression expr) {
        AssignableValue av = allocate(name, expr.getType());
        return evaluate(av, expr).read();
    }

    @Override
    public BytecodeExpression evaluateInto(BytecodeExpression expr) {
        if(expr instanceof LocalValue) {
            return expr;
        }
        AssignableValue av = allocate(expr.getType());
        return evaluate(av, expr).read();
    }

    @Override
    public void exec(final BytecodeSequence input) {
        body.add(input);
        if (input instanceof BytecodeExpression) {
            body.add(new PopSequence(((BytecodeExpression) input).getType()));
        }
    }

    @Override
    public void alias(String from, String to) {
        body.alias(from, to);
    }

    @Override
    public AssignableValue local(BytecodeExpression input) {
        AssignableValue av = allocate(input.getType());
        return evaluate(av, input);
    }

    @Override
    public void inc(final AssignableValue lv, final int count) {
        Preconditions.checkArgument(lv.getType().getJVMType().getSort() == Type.INT, "inc only applies to INT types: not %s", lv.getType().getJVMType());
        body.add(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.inc(lv, count);
            }
        });
    }

    @Override
    public ScopeBuilder scope() {
        return new BlockAdapter(source, body.child());
    }

    @Override
    public IterateBuilder iterate(BytecodeExpression iterable) {
        return new IterateBuilderAdapter(source, body, iterable);
    }

    @Override
    public CaseBuilder createCase() {
        return new CaseAdapter(source, body);
    }

    @Override
    public BytecodeExpression not(Location loc, final BytecodeExpression input) {
        return new BaseTypeExpression(BaseTypeAdapter.BOOLEAN) {
            @Override
            public void generate(CodeEmitter code) {
                MethodVisitor mv = code.getMethodVisitor();
                Label done = new Label();
                Label isTrue = new Label();
                Label isFalse = new Label();
                code.exec(input);
                input.getType().getComparisionAdapter().coerceBoolean(code, isTrue, isFalse, isFalse);
                mv.visitLabel(isFalse);
                code.emitBooleanConstant(true);
                mv.visitJumpInsn(Opcodes.GOTO, done);
                mv.visitLabel(isTrue);
                code.emitBooleanConstant(false);
                mv.visitLabel(done);
            }
        };
    }

    @Override
    public BytecodeExpression isNull(Location location, final BytecodeExpression input) {
        return new BaseTypeExpression(BaseTypeAdapter.BOOLEAN) {
            @Override
            public void generate(CodeEmitter code) {
                MethodVisitor mv = code.getMethodVisitor();
                code.exec(input);
                // it's surprising to not evaluate even if we know input isn't nullable
                if (input.getType().isPrimitive()) {
                    code.pop(input.getType());
                    code.emitBooleanConstant(false);
                } else {
                    Label done = new Label();
                    Label isNull = new Label();
                    mv.visitJumpInsn(Opcodes.IFNULL, isNull);
                    code.emitBooleanConstant(false);
                    mv.visitJumpInsn(Opcodes.GOTO, done);
                    mv.visitLabel(isNull);
                    code.emitBooleanConstant(true);
                    mv.visitLabel(done);
                }
            }
        };
    }

    @Override
    public BytecodeExpression coalesce(Location loc, BytecodeExpression... inputs) {
        return coalesce(loc, Arrays.asList(inputs));
    }

    @Override
    public BytecodeExpression coalesce(Location loc, final List<BytecodeExpression> inputs) {
        List<TypeWidget> widgets = Lists.newArrayList();
        for (BytecodeExpression expr : inputs) {
            widgets.add(expr.getType());
        }
        TypeWidget output = unify(widgets);
        return new BaseTypeExpression(output) {
            @Override
            public void generate(CodeEmitter code) {
                Label done = new Label();
                MethodVisitor mv = code.getMethodVisitor();
                boolean lastNullable = true;
                for (BytecodeExpression expr : inputs) {
                    Label isNull = new Label();
                    code.exec(expr);
                    if (code.cast(getType(), expr.getType(), isNull)) {
                        mv.visitJumpInsn(Opcodes.GOTO, done);
                        mv.visitLabel(isNull);
                    } else {
                        lastNullable = false;
                        break;
                    }
                }
                if (lastNullable) {
                    mv.visitInsn(Opcodes.ACONST_NULL);
                }
                mv.visitLabel(done);
            }
        };
    }

    @Override
    public AssignableValue local(String name) {
        return body.getLocal(name);
    }

    @Override
    public AssignableValue local(Location loc, String name) {
        return body.getLocal(name);
    }

    @Override
    public void set(Location loc, AssignableValue lv, BytecodeExpression expr) {
        body.add(lv.write(expr));
    }

    @Override
    public BytecodeExpression list(TypeWidget elementType) {
        return new ListTypeWidget(elementType).construct();
    }

    @Override
    public BytecodeExpression list(Location loc, final List<BytecodeExpression> args) {
        List<TypeWidget> types = Lists.newArrayList();
        for (BytecodeExpression e : args) {
            types.add(e.getType());
        }
        final TypeWidget unified = unify(types).boxed();
        final ListTypeWidget out = new ListTypeWidget(NotNullableTypeWidget.create(unified));
        return new BaseTypeExpression(out) {
            @Override
            public void generate(CodeEmitter code) {
                MethodVisitor mv = code.getMethodVisitor();
                code.exec(out.construct(constant(args.size())));
                for (BytecodeExpression expr : args) {
                    Label handleNull = new Label();
                    Label end = new Label();
                    mv.visitInsn(Opcodes.DUP);
                    code.exec(expr);
                    final TypeWidget type = expr.getType();
                    boolean nullable = code.cast(unified, type, handleNull);
                    mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Collection.class), "add", Type.getMethodDescriptor(Type.BOOLEAN_TYPE, Type.getType(Object.class)), true);
                    mv.visitInsn(Opcodes.POP);
                    if (nullable) {
                        mv.visitJumpInsn(Opcodes.GOTO, end);
                        mv.visitLabel(handleNull);
                        mv.visitInsn(Opcodes.POP);
                        mv.visitLabel(end);
                    }

                }
            }
        };
    }

    public BytecodeExpression newArray(Location loc, final TypeWidget elementType, final BytecodeExpression count) {
        return new BaseTypeExpression(NotNullableTypeWidget.create(new ArrayTypeWidget(Type.getType("[" + elementType.getJVMType().getDescriptor()), elementType))) {
            @Override
            public void generate(CodeEmitter code) {
                code.emitNewArray(elementType, count);
            }
        };
    }

    @Override
    public BytecodeExpression array(Location loc, TypeWidget elementType, final List<BytecodeExpression> args) {
        final BytecodeExpression create = newArray(loc, elementType, constant(args.size()));
        return new BaseTypeExpression(create.getType()) {
            @Override
            public void generate(CodeEmitter code) {
                CodeEmitter tmp = code.createScope();
                IndexAdapter idx = getType().getIndexAdapter();
                AssignableValue local = tmp.allocate(create);
                for (int i = 0; i < args.size(); ++i) {
                    tmp.exec(idx.index(local, constant(i)).write(args.get(i)));
                }
                tmp.exec(local.read());
                tmp.endScope();

            }
        };
    }

    @Override
    public BytecodeExpression invokeExact(Location loc, String methodName, Class<?> owner, TypeWidget returnType, BytecodeExpression... args) {
        return invokeExact(loc, methodName, owner, returnType, args == null ? ImmutableList.of() : Arrays.asList(args));
    }

    public BytecodeExpression invokeExact(Location loc, String methodName, Class<?> owner, TypeWidget returnType, List<BytecodeExpression> args) {
        return ExactInvocation.boundInvoke(owner.isInterface() ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
                methodName, adapt(owner, false), returnType, args).invoke(loc);
    }

    @Override
    public Invocable constructor(TypeWidget type, TypeWidget... argumentTypes) {
        return constructor(type, argumentTypes == null ? ImmutableList.of() : Arrays.asList(argumentTypes));
    }

    @Override
    public Invocable constructor(final TypeWidget type, List<TypeWidget> argumentTypes) {
        return new BytecodeInvocable(type, argumentTypes) {
            @Override
            protected void generate(Location loc, CodeEmitter code, List<BytecodeExpression> args) {
                code.exec(type.construct(args.toArray(new BytecodeExpression[args.size()])));
            }
        };
    }

    public BytecodeExpression transform(Location location, BytecodeExpression iterable, Invocable function) {
        ScopeBuilder scope = scope();
        IterateBuilder it = scope.iterate(iterable);
        BytecodeExpression list = scope.evaluateInto(list(function.getReturnType()));
        it.exec(it.findExactInvoker(Collection.class, "add", BaseTypeAdapter.BOOLEAN, AnyTypeWidget.getInstance()).invoke(location, list, function.invoke(location, it.getItem())));
        return scope.complete(it.build(list));
    }

    @Override
    public BytecodeExpression propertyValue(Location loc, BytecodeExpression target, String propertyName) {
        if (!target.getType().hasProperties()) {
            throw new ProgramCompileException(loc, "Cannot reference %s.%s", target.getType().getJVMType(), propertyName);
        }
        try {
            if (target.getType().isNullable()) {
                ScopeBuilder guard = scope();
                BytecodeExpression tgt = guard.evaluateInto(target);
                final AssignableValue property = target.getType().getPropertyAdapter().property(new NullCheckedEvaluatedExpression(tgt), propertyName);
                return guard.complete(guard.guarded(tgt, property));

            }
            return target.getType().getPropertyAdapter().property(target, propertyName);
        } catch (PropertyNotFoundException e) {
            throw new ProgramCompileException(loc, e.getMessage());
        }
    }

    @Override
    public BytecodeExpression indexValue(Location loc, BytecodeExpression target, BytecodeExpression index) {
        if (target.getType().isNullable()) {
            ScopeBuilder guard = scope();
            BytecodeExpression tgt = guard.evaluateInto(target);
            return guard.complete(guard.guarded(tgt, target.getType().getIndexAdapter().index(new NullCheckedEvaluatedExpression(tgt), index)));

        }
        return target.getType().getIndexAdapter().index(target, index);
    }

    @Override
    public BytecodeExpression guarded(final BytecodeExpression target, final BytecodeExpression ifTargetIsNotNull) {
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(ifTargetIsNotNull);
        if (!target.getType().isNullable()) {
            return ifTargetIsNotNull;
        }
        return new NullGuardedExpression(target, ifTargetIsNotNull);
    }

    @Override
    public BytecodeExpression cast(Location loc, TypeWidget type, BytecodeExpression input) {
        return new BytecodeCastExpression(type, input);
    }

    public BytecodeExpression cast(TypeWidget type, BytecodeExpression input) {
        return new BytecodeCastExpression(type, input);
    }

    @Override
    public BytecodeExpression resolve(Location loc, BytecodeExpression timeout, BytecodeExpression promise) {
        if (promise.getType().isPromise()) {
            return promise.getType().getPromiseAdapter().resolve(this, timeout, promise);
        } else {
            return promise;
        }
    }

    @Override
    public CatchBuilder tryCatchFinally() {
        return new BytecodeCatchBuilder(source, body);
    }

    @Override
    public BytecodeExpression evaluateTryCatch(Location loc, final BytecodeExpression expr) {
        // TODO: should we initialize the output value with a null?
        final ResultAdapter resultType = resultTypeFor(expr.getType());
        CatchBuilder tryCatch = tryCatchFinally();
        AssignableValue resultValue = body.allocate(resultType.getType());
        ScopedBuilder body = tryCatch.body();
        body.set(loc, resultValue, resultType.createSuccess(expr));
        ScopedBuilder catchBlock = tryCatch.on("$e", Throwable.class);
        catchBlock.set(loc, resultValue, resultType.createFailureThrowable(catchBlock.local("$e")));
        exec(tryCatch.build());
        return resultValue;
    }

    @Override
    public ScopeBuilder block() {
        return new BlockAdapter(source, body.block());
    }

    @Override
    public ScopeBuilder point() {
        return new BlockAdapter(source, body.point());
    }

    @Override
    public LocalCodeChunk getCode() {
        return body;
    }

    private static class NullGuardedExpression extends BaseTypeExpression {
        private final BytecodeExpression ifTargetIsNotNull;
        private final BytecodeExpression target;

        public NullGuardedExpression(BytecodeExpression target, BytecodeExpression ifTargetIsNotNull) {
            super(NullableTypeWidget.create(ifTargetIsNotNull.getType().boxed()));
            this.ifTargetIsNotNull = ifTargetIsNotNull;
            this.target = target;
        }

        @Override
        public void generate(CodeEmitter code) {
            final MethodVisitor mv = code.getMethodVisitor();
            Label isNull = new Label();
            Label done = new Label();
            code.exec(target);
            mv.visitJumpInsn(Opcodes.IFNULL, isNull);
            code.exec(ifTargetIsNotNull);
            code.cast(getType(), ifTargetIsNotNull.getType());
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(isNull);
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitLabel(done);
        }
    }

    private static final TypeWidget mapFieldWriter = new BaseTypeWidget(Type.getType(MapFieldWriter.class)) {
        @Override
        public YQLCoreType getValueCoreType() {
            return YQLCoreType.OBJECT;
        }

    };
    private static final TypeWidget mapType = new MapTypeWidget(Type.getType(RecordMapWrapper.class), BaseTypeAdapter.STRING, BaseTypeAdapter.ANY);


    private class DynamicOperation {
        final String fieldName;
        final BytecodeExpression value;

        public DynamicOperation(String fieldName, BytecodeExpression value) {
            this.fieldName = fieldName;
            this.value = value;
        }
    }

    private class DynamicExpressionRecordBuilder implements RecordBuilder {
        private final List<DynamicOperation> operationNodes = Lists.newArrayList();

        @Override
        public RecordBuilder add(Location loc, String fieldName, BytecodeExpression input) {
            operationNodes.add(new DynamicOperation(fieldName, input));
            return this;
        }

        @Override
        public RecordBuilder merge(Location loc, BytecodeExpression recordType) {
            operationNodes.add(new DynamicOperation("", recordType));
            return this;
        }

        @Override
        public BytecodeExpression build() {
            return new BaseTypeExpression(mapType) {
                @Override
                public void generate(CodeEmitter code) {
                    AssignableValue map = code.allocate(mapType.construct());
                    PropertyAdapter adapter = map.getType().getPropertyAdapter();
                    AssignableValue writer = code.allocate(mapFieldWriter.construct(new BytecodeCastExpression(new MapTypeWidget(Type.getType(Map.class), BaseTypeAdapter.STRING, BaseTypeAdapter.ANY), map)));
                    for(DynamicOperation op : operationNodes) {
                        if("".equals(op.fieldName)) {
                            BytecodeExpression value = op.value;
                            code.exec(value.getType().getPropertyAdapter().mergeIntoFieldWriter(value, writer));
                        } else {
                            String fieldName = op.fieldName;
                            BytecodeExpression value = op.value;
                            code.exec(adapter.property(map, fieldName).write(value));
                        }
                    }
                    code.exec(map.read());
                }
            };
        }
    }
    
    private static boolean isJavaFieldName(String fieldName) {
        if (fieldName.isEmpty()) {
            return false;
        } else {
            if (!Character.isJavaIdentifierStart(fieldName.charAt(0))) {
                return false;
            }
            if (fieldName.length() == 1) {
                return true;
            } else {
                for (int i = 1; i < fieldName.length(); i++) {
                    if (!Character.isJavaIdentifierPart(fieldName.charAt(i))) {
                        return false;
                    }
                }
                return true;
            }
        }     
    }

    private class ExpressionRecordBuilder implements RecordBuilder {
        private boolean dynamic = false;
        private RecordBuilder dynamicBuilder = null;

        private final Map<String, BytecodeExpression> fieldSettings = Maps.newLinkedHashMap();
        private final StructBuilder staticStructBuilder = createStruct();

        private void initDynamicBuilder() {
            // reset and convert ourselves to dynamic
            dynamic = true;
            dynamicBuilder = new DynamicExpressionRecordBuilder();
            // merge all of our existing properties to the dynamic builder
            for (Map.Entry<String, BytecodeExpression> entry:fieldSettings.entrySet()) {
                dynamicBuilder.add(Location.NONE, entry.getKey(), entry.getValue());
            }              
        }
        
        @Override
        public RecordBuilder add(Location loc, String fieldName, BytecodeExpression input) {
            if(dynamic) {
                dynamicBuilder.add(loc, fieldName, input);
                return this;
            }
            if (!isJavaFieldName(fieldName)) {
                initDynamicBuilder();
                dynamicBuilder.add(loc, fieldName, input);
                return this;
            }
            fieldSettings.put(fieldName, input);
            staticStructBuilder.add(fieldName, input.getType());
            return this;
        }

        @Override
        public RecordBuilder merge(Location loc, BytecodeExpression recordType) {
            if(dynamic) {
                dynamicBuilder.merge(loc, recordType);
                return this;
            }
            TypeWidget inputType = recordType.getType();
            if(!inputType.hasProperties()) {
                throw new UnsupportedOperationException("RecordBuilder.merge must take an argument with properties (e.g. a struct/record)");
            }
            PropertyAdapter inputProperties = inputType.getPropertyAdapter();
            if(!inputProperties.isClosed()) {
                initDynamicBuilder();
                dynamicBuilder.merge(loc, recordType);
                return this;
            }
            for(PropertyAdapter.Property property : inputProperties.getProperties()) {

                add(loc, property.name, guarded(recordType, inputProperties.property(recordType, property.name)));
            }
            return this;
        }

        @Override
        public BytecodeExpression build() {
            if(dynamic) {
                return dynamicBuilder.build();
            }
            return staticStructBuilder.build()
                    .getPropertyAdapter()
                    .construct(fieldSettings);
        }
    }
}
