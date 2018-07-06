/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.compiler.code.AnyTypeWidget;
import com.yahoo.yqlplus.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.GambitCreator;
import com.yahoo.yqlplus.compiler.code.GambitScope;
import com.yahoo.yqlplus.compiler.code.InvocableBuilder;
import com.yahoo.yqlplus.compiler.code.LambdaFactoryBuilder;
import com.yahoo.yqlplus.compiler.code.ObjectBuilder;
import com.yahoo.yqlplus.compiler.code.ReturnCode;
import com.yahoo.yqlplus.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.operator.OperatorStep;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.PhysicalOperator;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

public class TaskGenerator {
    private final ProgramGenerator programGenerator;
    private final LambdaFactoryBuilder builder;
    private final ScopedBuilder runBody;
    List<String> argumentNames = Lists.newArrayList();

    public TaskGenerator(ProgramGenerator program, GambitScope scope) {
        builder = scope.createLambdaBuilder(Runnable.class, "run", void.class, false);
        builder.addArgument("$program", program.getType());
        builder.addArgument("$context", scope.adapt(TaskContext.class, false));
        programGenerator = program;
        GambitCreator.CatchBuilder catcher = builder.tryCatchFinally();
        ScopedBuilder body = catcher.body();
        runBody = body.block();
        body.exec(new ReturnCode());
        ScopedBuilder handler = catcher.on("$e", Throwable.class);
        handler.exec(new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                code.exec(code.getLocal("$program"));
                code.exec(code.getLocal("$e"));
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(ProgramInvocation.class), "fail",
                        Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Throwable.class)), false);
                code.getMethodVisitor().visitInsn(Opcodes.RETURN);
            }
        });
        builder.exec(catcher.build());
    }

    public void addArgument(OperatorValue arg) {
        builder.addArgument(arg.getName(), programGenerator.getValue(arg));
        argumentNames.add(arg.getName());
    }

    public void executeStep(OperatorStep step) {
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(runBody);
        OperatorNode<PhysicalOperator> op = step.getCompute();
        OperatorValue output = step.getOutput();
        BytecodeExpression rootContext = runBody.propertyValue(op.getLocation(), runBody.local("$context"), "rootContext");
        switch (op.getOperator()) {
            case REQUIRED_ARGUMENT: {
                String nm = op.getArgument(0);
                YQLType ty = op.getArgument(1);
                BytecodeExpression expr = programGenerator.addProgramArgument(nm, ty);
                programGenerator.register(output, expr.getType());
                runBody.evaluateInto(output.getName(), expr);
                break;
            }
            case OPTIONAL_ARGUMENT: {
                String nm = op.getArgument(0);
                YQLType ty = op.getArgument(1);
                Object defaultValue = op.getArgument(2);
                BytecodeExpression expr = programGenerator.addProgramArgument(nm, ty, defaultValue);
                programGenerator.register(output, expr.getType());
                runBody.evaluateInto(output.getName(), expr);
                break;
            }
            case OUTPUT: {
                String nm = op.getArgument(0);
                OperatorNode<PhysicalExprOperator> value = op.getArgument(1);
                final BytecodeExpression nameExpr = runBody.constant(nm);
                GambitCreator.CatchBuilder catchBuilder = runBody.tryCatchFinally();
                ScopedBuilder body = catchBuilder.body();
                body.exec(body.invokeExact(op.getLocation(), "succeed", ProgramInvocation.class, BaseTypeAdapter.VOID, body.local("$program"), nameExpr, body.cast(op.getLocation(), AnyTypeWidget.getInstance(), compiler.evaluateExpression(body.local("$program"), rootContext, value))));
                ScopedBuilder fail = catchBuilder.on("$e", Throwable.class);
                fail.exec(body.invokeExact(op.getLocation(), "fail", ProgramInvocation.class, BaseTypeAdapter.VOID, body.local("$program"), nameExpr, body.cast(op.getLocation(), AnyTypeWidget.getInstance(), fail.local("$e"))));
                runBody.exec(catchBuilder.build());
                CompiledResultSetInfo info = new CompiledResultSetInfo(nm, Object.class);
                programGenerator.getResultSetInfos().add(info);
                break;
            }
            case END: {
                runBody.exec(runBody.invokeExact(op.getLocation(), "end", ProgramInvocation.class, BaseTypeAdapter.VOID, runBody.local("$program")));
                break;
            }
            case EVALUATE_GUARD:
            case EXECUTE:
            case EVALUATE: {
                OperatorNode<PhysicalExprOperator> contextExpr = op.getArgument(0);
                OperatorNode<PhysicalExprOperator> exprTree = op.getArgument(1);
                if (op.getOperator() == PhysicalOperator.EVALUATE_GUARD && exprTree.getOperator() != PhysicalExprOperator.ENFORCE_TIMEOUT) {
                    exprTree = OperatorNode.create(PhysicalExprOperator.ENFORCE_TIMEOUT, exprTree);
                }
                BytecodeExpression ctxExpr = compiler.evaluateExpression(runBody.local("$program"), rootContext, contextExpr);
                BytecodeExpression program = runBody.local("$program");
                InvocableBuilder stepInvocable = runBody.createInvocable();
                BytecodeExpression pgm = stepInvocable.addArgument("$program", program.getType());
                BytecodeExpression ctx = stepInvocable.addArgument("$context", ctxExpr.getType());
                PhysicalExprOperatorCompiler stepCompiler = new PhysicalExprOperatorCompiler(stepInvocable.block());
                BytecodeExpression outputExpression = stepCompiler.evaluateExpression(pgm, ctx, exprTree);
                if (op.getOperator() == PhysicalOperator.EXECUTE) {
                    GambitCreator.Invocable inc = stepInvocable.complete(outputExpression);
                    BytecodeExpression evaluatedExpr = inc.invoke(exprTree.getLocation() , program, ctxExpr);
                    runBody.exec(evaluatedExpr);
                } else if (outputExpression.getType().getValueCoreType() != YQLCoreType.VOID) {
                    BytecodeExpression expr = stepInvocable.evaluateTryCatch(op.getLocation(), outputExpression);
                    GambitCreator.Invocable inc = stepInvocable.complete(expr);

                    ObjectBuilder.FieldBuilder field = programGenerator.registerValue(output, expr.getType());
                    BytecodeExpression evaluatedExpr = inc.invoke(exprTree.getLocation(), program, ctxExpr);
                    runBody.set(Location.NONE, field.get(runBody.local("$program")), runBody.evaluateInto(output.getName(), evaluatedExpr));
                } else {
                    GambitCreator.Invocable inc = stepInvocable.complete(outputExpression);
                    BytecodeExpression evaluatedExpr = inc.invoke(exprTree.getLocation(), program, ctxExpr);
                    runBody.exec(evaluatedExpr);
                }
                break;
            }
            default:
                throw new UnsupportedOperationException("Unimplemented operator: " + op);
        }
    }


    public BytecodeExpression createRunnable(ScopedBuilder body, BytecodeExpression program, BytecodeExpression context, List<OperatorValue> args) {
        List<BytecodeExpression> exprs = Lists.newArrayList();
        exprs.add(program);
        exprs.add(context);
        for (OperatorValue arg : args) {
            exprs.add(body.local(arg.getName()));
        }
        return builder.exit().invoke(Location.NONE , exprs);
    }

    public ScopedBuilder getBody() {
        return runBody;
    }
}
