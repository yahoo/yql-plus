/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.tbin.TBinEncoder;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.bytecode.exprs.LocalVarExpr;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitCreator;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitScope;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ObjectBuilder;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ScopedBuilder;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.AnyTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.base.MapTypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;
import java.util.Map;

public class ProgramGenerator {
    private final ObjectBuilder.MethodBuilder readArguments;
    private final GambitCreator.ScopeBuilder readArgumentsBody;
    private final ObjectBuilder.MethodBuilder runMethod;
    private final GambitCreator.ScopeBuilder runBody;

    private final List<CompiledProgram.ArgumentInfo> argumentInfos = Lists.newArrayList();
    private final List<CompiledProgram.ResultSetInfo> resultSetInfos = Lists.newArrayList();

    private final Map<OperatorValue, TypeWidget> values = Maps.newIdentityHashMap();
    private int sym = 0;

    private final GambitScope scope;
    private final ObjectBuilder program;

    public ProgramGenerator(GambitScope scope) {
        this.scope = scope;
        this.program = scope.createObject(ProgramInvocation.class);
        program.implement(Runnable.class);
        this.readArguments = program.method("readArguments");
        this.readArguments.addArgument("$args", new MapTypeWidget(BaseTypeAdapter.STRING, AnyTypeWidget.getInstance()));
        this.readArgumentsBody = readArguments.block();
        this.readArguments.exit();
        this.runMethod = program.method("run");
        this.runBody = runMethod.block();
        this.runBody.alias("this", "$program");
        this.runMethod.exit();
        ObjectBuilder.MethodBuilder getSerializer = program.method("getNativeSerializer");
        final ObjectBuilder serializer = scope.createObject();
        serializer.implement(NativeSerialization.class);
        // void writeJson(JsonGenerator target, Object input);
        generateSerializationMethod(serializer, "writeJson", NativeEncoding.JSON, JsonGenerator.class);
        // void writeTBin(TBinEncoder target, Object input);
        generateSerializationMethod(serializer, "writeTBin", NativeEncoding.TBIN, TBinEncoder.class);
        final TypeWidget serializerType = serializer.type();
        getSerializer.exit(new BaseTypeExpression(scope.adapt(NativeSerialization.class, false)) {
            @Override
            public void generate(CodeEmitter code) {
                code.emitNew(serializerType.getJVMType().getInternalName());
            }
        });
    }

    private void generateSerializationMethod(ObjectBuilder serializer, String name, NativeEncoding encoding, Class<?> generatorType) {
        ObjectBuilder.MethodBuilder method = serializer.method(name);
        BytecodeExpression target = method.addArgument("target", scope.adapt(generatorType, false));
        BytecodeExpression input = method.addArgument("input", AnyTypeWidget.getInstance());
        SerializationAdapter adapter = input.getType().getSerializationAdapter(encoding);
        method.exec(adapter.serializeTo(input, target));
        method.exit();
    }

    public TypeWidget getValue(OperatorValue arg) {
        Preconditions.checkArgument(arg.getName() != null, "Passed OperatorValue missing name");
        Preconditions.checkState(values.containsKey(arg), "Missing OperatorValue used as argument '%s'", arg.getName());
        return values.get(arg);
    }


    public ObjectBuilder.FieldBuilder addJoin(String name, final TypeWidget joinType) {
        return program.finalField(name, joinType.construct(new LocalVarExpr(program.type(), "this")));
    }

    public List<CompiledProgram.ResultSetInfo> getResultSetInfos() {
        return resultSetInfos;
    }

    public List<CompiledProgram.ArgumentInfo> getArgumentInfos() {
        return argumentInfos;
    }

    public String gensym(String prefix) {
        return prefix + (++sym);
    }

    public void register(OperatorValue output, TypeWidget type) {
        if (output.getName() == null) {
            output.setName(gensym("local"));
        }
        values.put(output, type);
    }

    public TypeWidget getType() {
        return program.type();
    }

    public ScopedBuilder getBody() {
        return runBody;
    }

    public ObjectBuilder getProgram() {
        return program;
    }

    public ObjectBuilder.FieldBuilder registerValue(OperatorValue output, TypeWidget type) {
        register(output, type);
        ObjectBuilder.FieldBuilder outputField = program.field(output.getName(), type);
        outputField.addModifiers(Opcodes.ACC_VOLATILE);
        return outputField;
    }

    static final class MissingArgumentExpr extends BaseTypeExpression {
        private final BytecodeExpression name;
        private final BytecodeExpression yqlType;

        MissingArgumentExpr(TypeWidget type, BytecodeExpression name, BytecodeExpression yqlType) {
            super(type);
            this.name = name;
            this.yqlType = yqlType;
        }

        @Override
        public void generate(CodeEmitter code) {
            BytecodeExpression thisExpr = code.getLocal("this").read();
            thisExpr.generate(code);
            name.generate(code);
            yqlType.generate(code);
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    thisExpr.getType().getJVMType().getInternalName(),
                    "missingArgument",
                    Type.getMethodDescriptor(Type.VOID_TYPE, name.getType().getJVMType(), Type.getType(YQLType.class)), false);
            code.getMethodVisitor().visitInsn(Opcodes.RETURN);
        }
    }

    static final class ReadArgumentExpr extends BaseTypeExpression {
        private final BytecodeExpression arguments;
        private final BytecodeExpression name;
        private final BytecodeExpression defaultValue;

        ReadArgumentExpr(TypeWidget type, BytecodeExpression arguments, BytecodeExpression name, BytecodeExpression defaultValue) {
            super(type);
            this.arguments = arguments;
            this.name = name;
            this.defaultValue = defaultValue;
        }

        @Override
        public void generate(CodeEmitter code) {
            Label done = new Label();
            Label useDefault = new Label();
            MethodVisitor mv = code.getMethodVisitor();
            arguments.generate(code);
            name.generate(code);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Map.class), "get", Type.getMethodDescriptor(Type.getType(Object.class), Type.getType(Object.class)), true);
            code.cast(getType(), BaseTypeAdapter.ANY, useDefault);
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(useDefault);
            defaultValue.generate(code);
            code.cast(getType(), defaultValue.getType());
            mv.visitLabel(done);
        }
    }

    private BytecodeExpression defineArgument(String name, BytecodeExpression nameExpr, TypeWidget type, BytecodeExpression defaultValue) {
        ObjectBuilder.FieldBuilder argumentField = program.field(name, type);
        ReadArgumentExpr readExpr = new ReadArgumentExpr(type, readArgumentsBody.local("$args").read(), nameExpr, defaultValue);
        readArgumentsBody.exec(argumentField.get(readArgumentsBody.local("this")).write(readExpr));
        return argumentField.get(new LocalVarExpr(program.type(), "$program")).read();
    }


    public BytecodeExpression addProgramArgument(String nm, YQLType ty, Object defaultValue) {
        argumentInfos.add(new CompiledArgumentInfo(nm, ty, defaultValue));
        return defineArgument(nm, scope.constant(nm), scope.adapt(ty), scope.constant(defaultValue));
    }

    public BytecodeExpression addProgramArgument(String nm, YQLType ty) {
        argumentInfos.add(new CompiledArgumentInfo(nm, ty, null));
        final TypeWidget type = scope.adapt(ty);
        final BytecodeExpression name = scope.constant(nm);
        return defineArgument(nm, name, type, new MissingArgumentExpr(type, name, scope.constant(ty)));
    }

}
