/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.ProjectOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.logical.StatementOperatorTransform;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.List;

public class ReplaceSubqueryFilters extends StatementOperatorTransform {
    @Override
    public OperatorNode<ExpressionOperator> visitExpr(OperatorNode<ExpressionOperator> expr) {
        if (expr.getOperator() == ExpressionOperator.IN_QUERY || expr.getOperator() == ExpressionOperator.NOT_IN_QUERY) {
            ExpressionOperator newOperator = expr.getOperator() == ExpressionOperator.IN_QUERY ? ExpressionOperator.IN : ExpressionOperator.NOT_IN;
            OperatorNode<ExpressionOperator> selector = expr.getArgument(0);
            OperatorNode<SequenceOperator> query = expr.getArgument(1);
            if (query.getOperator() != SequenceOperator.PROJECT) {
                throw new ProgramCompileException(expr.getLocation(), "SELECT used as argument to IN must be a projection to identify field to use");
            }
            OperatorNode<SequenceOperator> targetQuery = query.getArgument(0);
            List<OperatorNode<ProjectOperator>> projection = query.getArgument(1);
            OperatorNode<ProjectOperator> field = projection.get(0);
            if (field.getOperator() != ProjectOperator.FIELD) {
                throw new ProgramCompileException("Unexpected argument to PROJECT:" + field.getOperator());
            }
            OperatorNode<ExpressionOperator> matchExpression = field.getArgument(0);
            OperatorNode<SequenceOperator> extract = OperatorNode.create(query.getLocation(), query.getAnnotations(), SequenceOperator.EXTRACT, targetQuery.transform(this), matchExpression);
            String name = gensym();
            add(OperatorNode.create(query.getLocation(), StatementOperator.EXECUTE, extract, name));
            return OperatorNode.create(newOperator, selector.transform(this), OperatorNode.create(ExpressionOperator.VARREF, name));
        }
        return expr.transform(this);
    }

    List<OperatorNode<StatementOperator>> statements;

    private String gensym() {
        if (statements == null) {
            throw new ProgramCompileException("Invariant violation: ReplaceSubqueryFilters is not processing a PROGRAM when attempting to gensym");
        }
        return "subquery$" + statements.size();
    }

    private void add(OperatorNode<StatementOperator> statement) {
        if (statements == null) {
            throw new ProgramCompileException("Invariant violation: ReplaceSubqueryFilters is not processing a PROGRAM when attempting to add a statement");
        }
        statements.add(statement);
    }

    public OperatorNode<StatementOperator> visitStatement(OperatorNode<StatementOperator> node) {
        switch (node.getOperator()) {
            case PROGRAM: {
                List<OperatorNode<StatementOperator>> stmts = node.getArgument(0);
                this.statements = Lists.newArrayList();
                for (OperatorNode<StatementOperator> stmt : stmts) {
                    // if visitStatement adds any statements they will be inserted before this one
                    add(visitStatement(stmt));
                }
                OperatorNode<StatementOperator> output = OperatorNode.create(node.getLocation(), node.getAnnotations(), node.getOperator(), this.statements);
                this.statements = null;
                return output;
            }
            case EXECUTE: {
                return node.transform(this);
            }
            case ARGUMENT:
            case DEFINE_VIEW:
            case OUTPUT:
            case COUNT:
                return node;
            default:
                throw new ProgramCompileException("Unknown StatementOperator: " + node);
        }
    }


}
