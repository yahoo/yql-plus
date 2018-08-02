/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.internal.code.CodeOutput;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.operator.OperatorStep;
import com.yahoo.yqlplus.operator.OperatorValue;

import java.util.List;

public class PlanPrinter {
    public CodeOutput dump(OperatorNode<TaskOperator> plan) {
        return dump(new CodeOutput(), plan);
    }

    public CodeOutput dump(CodeOutput output, OperatorNode<TaskOperator> plan) {
        switch (plan.getOperator()) {
            case PLAN: {
                OperatorNode<TaskOperator> start = plan.getArgument(0);
                List<OperatorNode<TaskOperator>> argspec = plan.getArgument(1);
                if (!argspec.isEmpty()) {
                    output.println("arguments {");
                    output.indent();
                    for (OperatorNode<TaskOperator> arg : argspec) {
                        String name = arg.getArgument(0);
                        YQLType type = arg.getArgument(1);
                        output.println("%s %s;", type.toString(), name);
                    }
                    output.dedent();
                    output.println("}");
                }
                List<OperatorNode<TaskOperator>> ops = plan.getArgument(2);
                for (OperatorNode<TaskOperator> op : ops) {
                    dump(output, op);
                }
                output.println("function start() {", start);
                output.indent();
                dumpCall(output, start);
                output.dedent();
                output.println("}");
                break;
            }
            case RUN: {
                String name = plan.getArgument(0);
                List<OperatorValue> values = plan.getArgument(1);
                List<OperatorStep> calls = plan.getArgument(2);
                OperatorNode<TaskOperator> next = plan.getArgument(3);
                output.println("function %s(%s) {", name, toArglist(values));
                output.indent();
                int local = 0;
                for (OperatorStep call : calls) {
                    OperatorValue val = call.getOutput();
                    if (val.getName() == null) {
                        val.setName("local" + (local++));
                    }
                    if (val.hasDatum()) {
                        output.println("(%s <- (%s)).then(next);", val.getName(), call.getCompute());
                    } else {
                        output.println("(%s).then(next);", call.getCompute());
                    }
                }
                dumpCall(output, next);
                output.dedent();
                output.println("}");
                output.println("");
                break;
            }
            case JOIN:
                String name = plan.getArgument(0);
                List<OperatorValue> values = plan.getArgument(1);
                OperatorNode<TaskOperator> next = plan.getArgument(2);
                int count = plan.getArgument(3);
                output.println("function %s(%s) {", name, toArglist(values));
                output.indent();
                output.println("count = %d;", count);
                dumpCall(output, next);
                output.dedent();
                output.println("}");
                output.println("");
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return output;
    }

    private void dumpCall(CodeOutput output, OperatorNode<TaskOperator> plan) {
        switch (plan.getOperator()) {
            case READY:
            case CALL: {
                String name = plan.getArgument(0);
                List<OperatorValue> args = plan.getArgument(1);
                output.println("%s(%s);", name, toArglist(args));
                break;
            }
            case PARALLEL: {
                List<OperatorNode<TaskOperator>> calls = plan.getArgument(0);
                for (int i = 0; i < calls.size() - 1; ++i) {
                    output.println("fork {");
                    output.indent();
                    dumpCall(output, calls.get(i));
                    output.dedent();
                    output.println("}");
                }
                dumpCall(output, calls.get(calls.size() - 1));
                break;
            }
            case END:
                output.println("end();");
                break;
            default:
                throw new UnsupportedOperationException(plan.getOperator().name());
        }
    }

    private String toArglist(List<OperatorValue> values) {
        return Joiner.on(", ").join(Iterables.transform(values, new Function<OperatorValue, Object>() {
            @Override
            public Object apply(OperatorValue input) {
                Preconditions.checkArgument(input.getName() != null, "OperatorValue argument not assigned name");
                return input.getName();
            }
        }));
    }
}
