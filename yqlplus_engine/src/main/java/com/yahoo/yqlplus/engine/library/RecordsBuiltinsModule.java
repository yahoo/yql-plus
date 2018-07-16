/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.library;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.CompileContext;
import com.yahoo.yqlplus.engine.ModuleType;
import com.yahoo.yqlplus.engine.StreamValue;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.PhysicalProjectOperator;

import java.util.List;

public class RecordsBuiltinsModule implements ModuleType {
    @Override
    public OperatorNode<PhysicalExprOperator> call(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments) {
        return callInRowContext(location, context, name, arguments, null);
    }

    @Override
    public OperatorNode<PhysicalExprOperator> callInRowContext(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row) {
        if("merge".equals(name)) {
            List<OperatorNode<PhysicalExprOperator>> args = context.evaluateAllInRowContext(arguments, row);
            List<OperatorNode<PhysicalProjectOperator>> ops = Lists.newArrayListWithExpectedSize(args.size());
            for(OperatorNode<PhysicalExprOperator> arg : args) {
                ops.add(OperatorNode.create(arg.getLocation(), PhysicalProjectOperator.MERGE, arg));
            }
            return OperatorNode.create(PhysicalExprOperator.PROJECT, ops);
        } else if ("map".equals(name)) {
            List<OperatorNode<PhysicalExprOperator>> args = context.evaluateAllInRowContext(arguments, row);
            List<OperatorNode<PhysicalProjectOperator>> ops = Lists.newArrayListWithExpectedSize(args.size());
            for(OperatorNode<PhysicalExprOperator> arg : args) {
                ops.add(OperatorNode.create(arg.getLocation(), PhysicalProjectOperator.MERGE, arg));
            }
            OperatorNode<PhysicalExprOperator> projectNode = OperatorNode.create(PhysicalExprOperator.PROJECT, ops);
            projectNode.putAnnotation("project:type", "map");
            return projectNode;
        }
        throw new ProgramCompileException(location, "Unknown records function '%s'", name);    }

    @Override
    public OperatorNode<PhysicalExprOperator> property(Location location, CompileContext context, String name) {
        return null;
    }

    @Override
    public StreamValue pipe(Location location, CompileContext context, String name, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments) {
        return null;
    }
}
