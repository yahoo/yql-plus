/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.engine.internal.plan.ast.*;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.ProjectOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.SortOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;
import java.util.Set;

public abstract class PlanChain {
    private ContextPlanner context;

    protected PlanChain(ContextPlanner context) {
        this.context = context;
    }

    public static class LocalChainState {
        OperatorNode<ExpressionOperator> filter;
        List<OperatorNode<SequenceOperator>> transforms = Lists.newArrayList();
        Set<OperatorNode<SequenceOperator>> handled = Sets.newIdentityHashSet();

        boolean filtered = false;

        public OperatorNode<ExpressionOperator> getFilter() {
            return filter;
        }

        public void setFilterHandled(boolean filtered) {
            this.filtered = filtered;
        }

    }

    public StreamValue execute(OperatorNode<SequenceOperator> query) {
        LocalChainState state = new LocalChainState();
        return execute(state, query);
    }

    private static class FilterDependency {
        final OperatorNode<SequenceOperator> filterNode;
        final OperatorNode<ExpressionOperator> rowIndependentFilter;

        public FilterDependency(OperatorNode<SequenceOperator> filterNode, OperatorNode<ExpressionOperator> rowIndependentFilter) {
            this.filterNode = filterNode;
            this.rowIndependentFilter = rowIndependentFilter;
        }
    }

    private boolean isRowDependentAny(List<OperatorNode<ExpressionOperator>> args) {
        for(OperatorNode<ExpressionOperator> arg : args) {
            if(isRowDependent(arg)) {
                return true;
            }
        }
        return false;
    }

    private boolean isRowDependent(OperatorNode<ExpressionOperator> filter) {
        switch(filter.getOperator()) {
            case LITERAL:
            case NULL:
                return false;
            case AND:
            case OR:
                return isRowDependentAny(filter.getArgument(0));
            // binary operations
            case EQ:
            case NEQ:
            case LT:
            case GT:
            case LTEQ:
            case GTEQ:
            case NOT_IN:
            case LIKE:
            case MATCHES:
            case NOT_MATCHES:
            case NOT_LIKE:
            case CONTAINS:
            case ADD:
            case SUB:
            case MULT:
            case DIV:
            case MOD:
            case IN:
                return isRowDependent(filter.getArgument(0)) || isRowDependent(filter.getArgument(1));
            // unary operations
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT:
            case NEGATE:
                return isRowDependent(filter.getArgument(0));
            case MAP:
                return isRowDependentAny(filter.getArgument(1));
            case ARRAY:
                return isRowDependentAny(filter.getArgument(0));
            case INDEX:
                return isRowDependent(filter.getArgument(0)) || isRowDependent(filter.getArgument(1));
            case PROPREF:
                return isRowDependent(filter.getArgument(0));
            case VARREF:
                return false;
            case CALL:
            case READ_RECORD:
            case READ_FIELD:
            case READ_MODULE:
                return true;
            default:
                // should maybe blow up in the default case since it means we have a new operator or one that should be
                // factored out by the time this runs
                // it's safe to assume it IS row dependent
                return true;
        }
    }

    private void visitFilter(OperatorNode<ExpressionOperator> filter, List<OperatorNode<ExpressionOperator>> dependent, List<OperatorNode<ExpressionOperator>> independent) {
        switch(filter.getOperator()) {
            case AND: {
                List<OperatorNode<ExpressionOperator>> targets = filter.getArgument(0);
                for(OperatorNode<ExpressionOperator> target : targets) {
                    visitFilter(target, dependent, independent);
                }
                break;
            }
            default: {
                if(isRowDependent(filter)) {
                    dependent.add(filter);
                } else {
                    independent.add(filter);
                }
            }
        }
    }

    private OperatorNode<ExpressionOperator> and(List<OperatorNode<ExpressionOperator>> inputs) {
        if(inputs.size() < 2) {
            return inputs.get(0);
        } else {
            return OperatorNode.create(ExpressionOperator.AND, inputs);
        }
    }

    private OperatorNode<SequenceOperator> makeFilter(OperatorNode<SequenceOperator> old, List<OperatorNode<ExpressionOperator>> inputs, OperatorNode<SequenceOperator> source) {
        if(inputs.isEmpty()) {
            return source;
        }
        return OperatorNode.create(old.getLocation(), old.getAnnotations(), SequenceOperator.FILTER, source, and(inputs));
    }

    private FilterDependency transformRowIndependentFilter(OperatorNode<SequenceOperator> filterNode) {
        // TODO: transform this further so that row-independent values are executed only once even when they can't be extracted (e.g. ORs)
        List<OperatorNode<ExpressionOperator>> dependent = Lists.newArrayList();
        List<OperatorNode<ExpressionOperator>> independent = Lists.newArrayList();
        OperatorNode<SequenceOperator> source = filterNode.getArgument(0);
        OperatorNode<ExpressionOperator> expr = filterNode.getArgument(1);
        visitFilter(expr, dependent, independent);
        if(independent.isEmpty()) {
            return new FilterDependency(filterNode, null);
        }
        return new FilterDependency(
                makeFilter(filterNode, dependent, source),
                and(independent)
        );
    }
    
    public StreamValue execute(LocalChainState state, OperatorNode<SequenceOperator> query) {
        query = enter(query);
        switch (query.getOperator()) {
            case PROJECT: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                List<OperatorNode<ProjectOperator>> fields = query.getArgument(1);
                StreamValue result = execute(state, source);
                result.add(query.getLocation(), StreamOperator.TRANSFORM, compileProjection(fields));
                return result;
            }
            case EXTRACT: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                OperatorNode<ExpressionOperator> extract = query.getArgument(1);
                StreamValue result = execute(state, source);
                result.add(query.getLocation(), StreamOperator.TRANSFORM, compileExtract(extract));
                return result;
            }
            case FILTER: {
                FilterDependency dep = transformRowIndependentFilter(query);
                if(dep.rowIndependentFilter != null) {
                    StreamValue result = execute(state, dep.filterNode);
                    OperatorNode<PhysicalExprOperator> independent = context.evaluate(dep.rowIndependentFilter);
                    OperatorNode<PhysicalExprOperator> expr = OperatorNode.create(query.getLocation(), PhysicalExprOperator.IF, independent,
                            OperatorNode.create(PhysicalExprOperator.VALUE, result.materializeIf(independent)),
                            context.constant(ImmutableList.of()));
                    return StreamValue.iterate(context, expr);
                } else {
                    OperatorNode<SequenceOperator> source = query.getArgument(0);
                    state.filter = query.getArgument(1);
                    StreamValue result = execute(state, source);
                    if (!state.filtered) {
                        result.add(query.getLocation(), StreamOperator.FILTER, compileFilter(state.filter));
                    }
                    return result;
                }
            }
            case SORT: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                List<OperatorNode<SortOperator>> orderby = query.getArgument(1);
                state.transforms.add(query);
                StreamValue result = execute(state, source);
                if (!state.handled.contains(query)) {
                    result.add(query.getLocation(), StreamOperator.ORDERBY, compileComparator(orderby));
                }
                return result;
            }
            case LIMIT: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                OperatorNode<ExpressionOperator> limit = query.getArgument(1);
                state.transforms.add(query);
                StreamValue result = execute(state, source);
                if (!state.handled.contains(query)) {
                    result.add(query.getLocation(), StreamOperator.LIMIT, programEvaluate(limit));
                }
                return result;
            }
            case OFFSET: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                OperatorNode<ExpressionOperator> limit = query.getArgument(1);
                state.transforms.add(query);
                StreamValue result = execute(state, source);
                if (!state.handled.contains(query)) {
                    result.add(query.getLocation(), StreamOperator.OFFSET, programEvaluate(limit));
                }
                return result;
            }
            case SLICE: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                OperatorNode<ExpressionOperator> offset = query.getArgument(1);
                OperatorNode<ExpressionOperator> limit = query.getArgument(2);
                state.transforms.add(query);
                StreamValue result = execute(state, source);
                if (!state.handled.contains(query)) {
                    result.add(query.getLocation(), StreamOperator.SLICE, programEvaluate(offset), programEvaluate(limit));
                }
                return result;
            }
            case TIMEOUT: {
                OperatorNode<SequenceOperator> source = query.getArgument(0);
                OperatorNode<ExpressionOperator> timeout = query.getArgument(1);
                ContextPlanner oldContext = context;
                context = oldContext.timeout(timeout);
                StreamValue result = execute(state, source);
                OperatorValue output = result.materialize();
                result = StreamValue.iterate(oldContext, output);
                return result;
            }
            default: {
                return executeSource(context, state, query);
            }
        }
    }

    private OperatorNode<PhysicalExprOperator> programEvaluate(OperatorNode<ExpressionOperator> offset) {
        return context.evaluate(offset);
    }

    private OperatorNode<FunctionOperator> compileComparator(List<OperatorNode<SortOperator>> orderby) {
        ExprScope function = new ExprScope();
        function.addArgument("left");
        function.addArgument("right");
        DynamicExpressionEvaluator leftEval = new DynamicExpressionEvaluator(context, OperatorNode.create(PhysicalExprOperator.LOCAL, "left"));
        DynamicExpressionEvaluator rightEval = new DynamicExpressionEvaluator(context, OperatorNode.create(PhysicalExprOperator.LOCAL, "right"));
        List<OperatorNode<PhysicalExprOperator>> compares = Lists.newArrayList();
        for (OperatorNode<SortOperator> clause : orderby) {
            OperatorNode<ExpressionOperator> expr = clause.getArgument(0);
            if (clause.getOperator() == SortOperator.ASC) {
                compares.add(OperatorNode.create(PhysicalExprOperator.COMPARE, leftEval.apply(expr), rightEval.apply(expr)));
            } else {
                compares.add(OperatorNode.create(PhysicalExprOperator.COMPARE, rightEval.apply(expr), leftEval.apply(expr)));
            }
        }
        OperatorNode<PhysicalExprOperator> functionBody;
        if (compares.size() > 1) {
            functionBody = OperatorNode.create(PhysicalExprOperator.MULTICOMPARE, compares);
        } else {
            functionBody = compares.get(0);
        }
        return function.createFunction(functionBody);
    }

    private OperatorNode<FunctionOperator> compileProjection(List<OperatorNode<ProjectOperator>> fields) {
        List<String> keys = Lists.newArrayList();
        List<OperatorNode<PhysicalExprOperator>> fieldValues = Lists.newArrayList();
        ExprScope scoper = new ExprScope();
        scoper.addArgument("item");
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(context, OperatorNode.create(PhysicalExprOperator.LOCAL, "item"));
        for (OperatorNode<ProjectOperator> field : fields) {
            switch (field.getOperator()) {
                case FIELD: {
                    OperatorNode<ExpressionOperator> expr = field.getArgument(0);
                    String name = field.getArgument(1);
                    keys.add(name);
                    fieldValues.add(eval.apply(expr));
                    break;
                }
                case MERGE_RECORD: {
                    return compileProjectOperator(fields);
                }
                default:
                    throw new UnsupportedOperationException("Unsupported project: " + field.getOperator());
            }
        }
        return scoper.createFunction(OperatorNode.create(PhysicalExprOperator.RECORD, keys, fieldValues));
    }

    private OperatorNode<FunctionOperator> compileProjectOperator(List<OperatorNode<ProjectOperator>> fields) {
        List<OperatorNode<PhysicalProjectOperator>> operations = Lists.newArrayList();
        ExprScope scoper = new ExprScope();
        scoper.addArgument("item");
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(context, OperatorNode.create(PhysicalExprOperator.LOCAL, "item"));
        for (OperatorNode<ProjectOperator> field : fields) {
            switch (field.getOperator()) {
                case FIELD: {
                    OperatorNode<ExpressionOperator> expr = field.getArgument(0);
                    String name = field.getArgument(1);
                    operations.add(OperatorNode.create(PhysicalProjectOperator.FIELD, eval.apply(expr), name));
                    break;
                }
                case MERGE_RECORD: {
                    String recordName = field.getArgument(0);
                    OperatorNode<ExpressionOperator> expr = OperatorNode.create(field.getLocation(), field.getAnnotations(), ExpressionOperator.READ_RECORD, recordName);
                    operations.add(OperatorNode.create(PhysicalProjectOperator.MERGE, eval.apply(expr)));
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported project: " + field.getOperator());
            }
        }
        return scoper.createFunction(OperatorNode.create(PhysicalExprOperator.PROJECT, operations));
    }

    private OperatorNode<FunctionOperator> compileExtract(OperatorNode<ExpressionOperator> expr) {
        ExprScope scoper = new ExprScope();
        scoper.addArgument("item");
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(context, OperatorNode.create(PhysicalExprOperator.LOCAL, "item"));
        OperatorNode<PhysicalExprOperator> output = eval.apply(expr);
        return scoper.createFunction(output);
    }

    protected OperatorNode<FunctionOperator> compileFilter(OperatorNode<ExpressionOperator> filter) {
        ExprScope scoper = new ExprScope();
        scoper.addArgument("item");
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(context, OperatorNode.create(PhysicalExprOperator.LOCAL, "item"));
        OperatorNode<PhysicalExprOperator> predicate = eval.apply(filter);
        predicate = OperatorNode.create(filter.getLocation(), PhysicalExprOperator.BOOL, predicate);
        return scoper.createFunction(predicate);
    }

    protected OperatorNode<SequenceOperator> enter(OperatorNode<SequenceOperator> operator) {
        return operator;
    }

    protected abstract StreamValue executeSource(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> query);


}
