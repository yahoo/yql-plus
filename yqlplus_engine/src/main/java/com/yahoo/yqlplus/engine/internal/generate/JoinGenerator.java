/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.code.GambitScope;
import com.yahoo.yqlplus.compiler.code.ObjectBuilder;
import com.yahoo.yqlplus.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.language.parser.Location;
import org.objectweb.asm.Opcodes;

import java.util.List;
import java.util.Map;

public class JoinGenerator {
    private final ObjectBuilder builder;
    private final ObjectBuilder.MethodBuilder execMethod;
    private final GambitCreator.ScopeBuilder execBody;
    private final Map<String, ObjectBuilder.FieldBuilder> parameters = Maps.newIdentityHashMap();

    public JoinGenerator(TypeWidget programType, GambitScope environment, final int count) {
        this.builder = environment.createObject(JoinTask.class);
        this.builder.addParameter("$program", programType);
        ObjectBuilder.ConstructorBuilder constructor = this.builder.getConstructor();
        constructor.invokeSpecial(JoinTask.class, constructor.constant(count));
        this.execMethod = builder.method("exec");
        execMethod.addModifiers(Opcodes.ACC_SYNCHRONIZED);
        execBody = execMethod.block();
        execMethod.exit();
    }

    public void addValue(String name, TypeWidget type) {
        parameters.put(name, builder.field(name, type));
        execBody.evaluateInto(name, parameters.get(name).get(execBody.local("this")).read());
    }

    public GambitCreator.ScopeBuilder getBody() {
        return execBody;
    }

    public BytecodeExpression createRunnable(ScopedBuilder body, BytecodeExpression joinExpr, List<OperatorValue> valueList) {
        if (valueList.isEmpty()) {
            return joinExpr;
        }
        final List<String> argNames = Lists.newArrayList();
        final List<TypeWidget> argValues = Lists.newArrayList();
        final List<BytecodeExpression> arguments = Lists.newArrayList();
        for (OperatorValue value : valueList) {
            Preconditions.checkArgument(value.getName() != null, "OperatorValue in valueList not assigned name");
            argNames.add(value.getName());
            argValues.add(parameters.get(value.getName()).getType());
            arguments.add(body.local(value.getName()));
        }
        final String methodName = "send" + Joiner.on("$").join(argNames);
        if (!builder.hasMethod(methodName)) {
            ObjectBuilder.MethodBuilder method = builder.method(methodName);
            method.addModifiers(Opcodes.ACC_SYNCHRONIZED);
            List<BytecodeExpression> args = Lists.newArrayList();
            for (int i = 0; i < argNames.size(); ++i) {
                args.add(method.addArgument(argNames.get(i), argValues.get(i)));
            }
            for (int i = 0; i < valueList.size(); ++i) {
                method.exec(parameters.get(valueList.get(i).getName()).get(method.local("this")).write(args.get(i)));
            }
            method.exit();
        }
        final GambitCreator.Invocable send = builder.methodInvocable(methodName, joinExpr);
        body.exec(body.invoke(Location.NONE, send, arguments));
        return joinExpr;
    }

    public TypeWidget getType() {
        return builder.type();
    }

    public ObjectBuilder getBuilder() {
        return builder;
    }
}
