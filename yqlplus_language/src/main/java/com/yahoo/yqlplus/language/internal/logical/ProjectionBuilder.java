/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.internal.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.ProjectOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.Map;
import java.util.Set;

public class ProjectionBuilder {
    private Map<String, OperatorNode<ExpressionOperator>> fields = Maps.newLinkedHashMap();
    private Set<String> aliasNames = Sets.newHashSet();

    public void addField(String name, OperatorNode<ExpressionOperator> expr) {
        String aliasName = name;
        if (name == null) {
            name = assignName(expr);
        }
        if (fields.containsKey(name)) {
            throw new ProgramCompileException(expr.getLocation(), "Field alias '%s' already defined", name);
        }
        fields.put(name, expr);
        if (aliasName != null) {
            // Store use
            aliasNames.add(aliasName);
        }
    }

    public boolean isAlias(String name) {
        return aliasNames.contains(name);
    }

    private String assignName(OperatorNode<ExpressionOperator> expr) {
        String baseName = "expr";
        switch (expr.getOperator()) {
            case PROPREF:
                baseName = expr.getArgument(1);
                break;
            case READ_RECORD:
                baseName = expr.getArgument(0);
                break;
            case READ_FIELD:
                baseName = expr.getArgument(1);
                break;
            case VARREF:
                baseName = expr.getArgument(0);
                break;
            // fall through, leaving baseName alone
        }
        int c = 0;
        String candidate = baseName;
        while (fields.containsKey(candidate)) {
            candidate = baseName + (++c);
        }
        return candidate;
    }

    public OperatorNode<SequenceOperator> make(OperatorNode<SequenceOperator> target) {
        ImmutableList.Builder<OperatorNode<ProjectOperator>> lst = ImmutableList.builder();
        for (Map.Entry<String, OperatorNode<ExpressionOperator>> e : fields.entrySet()) {
            if (e.getKey().startsWith("*")) {
                lst.add(OperatorNode.create(ProjectOperator.MERGE_RECORD, (Object)e.getValue().getArgument(0)));
            } else {
                lst.add(OperatorNode.create(ProjectOperator.FIELD, e.getValue(), e.getKey()));
            }
        }
        return OperatorNode.create(SequenceOperator.PROJECT, target, lst.build());
    }

}
