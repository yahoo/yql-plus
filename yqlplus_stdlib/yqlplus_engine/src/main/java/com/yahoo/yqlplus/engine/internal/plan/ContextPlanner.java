/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.GambitScope;
import com.yahoo.yqlplus.engine.internal.java.sequences.Sequences;
import com.yahoo.yqlplus.engine.internal.plan.ast.ExprScope;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorStep;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamValue;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.rules.ReadFieldAliasAnnotate;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ContextPlanner implements DynamicExpressionEnvironment {
    private final ProgramPlanner program;
    private final OperatorNode<PhysicalExprOperator> contextExpr;
    private final DynamicExpressionEvaluator eval;

    public ContextPlanner(ProgramPlanner program) {
        this.program = program;
        this.contextExpr = OperatorNode.create(PhysicalExprOperator.ROOT_CONTEXT);
        this.eval = new DynamicExpressionEvaluator(this);
    }

    ContextPlanner(ProgramPlanner program, OperatorValue context, DynamicExpressionEvaluator eval) {
        this.program = program;
        this.contextExpr = OperatorNode.create(PhysicalExprOperator.VALUE, context);
        this.eval = eval;
    }

    public OperatorValue evaluateValue(OperatorNode<PhysicalExprOperator> input) {
        switch (input.getOperator()) {
            case VALUE:
                return (OperatorValue) input.getArgument(0);
            case CONSTANT:
                return constantValue(input.getArgument(0));
            default:
                return OperatorStep.create(program.getValueTypeAdapter(), input.getLocation(), PhysicalOperator.EVALUATE_GUARD, contextExpr, input);
        }
    }

    @Override
    public OperatorNode<PhysicalExprOperator> evaluate(OperatorNode<ExpressionOperator> value) {
        return eval.apply(value);
    }

    @Override
    public OperatorNode<PhysicalExprOperator> property(Location location, List<String> path) {
        return program.property(this, location, path);
    }

    public OperatorValue constantValue(Object value) {
        return OperatorStep.create(program.getValueTypeAdapter(), PhysicalOperator.EVALUATE, contextExpr, OperatorNode.create(PhysicalExprOperator.CONSTANT, getValueTypeAdapter().inferConstantType(value), value));
    }

    public OperatorNode<PhysicalExprOperator> computeExpr(OperatorNode<PhysicalExprOperator> op) {
        if (op.getOperator() == PhysicalExprOperator.VALUE || op.getOperator() == PhysicalExprOperator.CONSTANT) {
            return op;
        }
        return OperatorNode.create(op.getLocation(), PhysicalExprOperator.VALUE, evaluateValue(op));
    }


    @Override
    public OperatorNode<PhysicalExprOperator> call(OperatorNode<ExpressionOperator> call) {
        return program.call(this, call);
    }

    @Override
    public OperatorNode<PhysicalExprOperator> call(OperatorNode<ExpressionOperator> call, OperatorNode<PhysicalExprOperator> row) {
        return program.call(this, call, row);
    }


    private StreamValue executePipe(OperatorNode<SequenceOperator> source) {
        OperatorNode<SequenceOperator> input = source.getArgument(0);
        List<String> path = source.getArgument(1);
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(2);
        return program.pipe(source.getLocation(), this, execute(input), path, args);
    }

    StreamValue executeJoin(OperatorNode<SequenceOperator> join, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> query) {
        SequenceOperator operator = join.getOperator();
        ReadFieldAliasAnnotate.RowType leftRowType = ReadFieldAliasAnnotate.RowType.getLeftType(join);
        ReadFieldAliasAnnotate.RowType rightRowType = ReadFieldAliasAnnotate.RowType.getRightType(join);

        StreamValue rightSide = executeJoinRightQuery(leftSide, joinExpression, query);

        List<OperatorNode<PhysicalExprOperator>> leftKey = Lists.newArrayList();
        List<OperatorNode<PhysicalExprOperator>> rightKey = Lists.newArrayList();
        DynamicExpressionEvaluator eval = new DynamicExpressionEvaluator(this, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"));
        List<JoinExpression> joinExpressions =  JoinExpression.parse(joinExpression);
        for(JoinExpression expr : joinExpressions) {
            leftKey.add(eval.apply(expr.left));
            rightKey.add(eval.apply(expr.right));
        }

        ExprScope function = new ExprScope();
        function.addArgument("$row");

        OperatorNode<FunctionOperator> leftKeyFunction =
                function.createFunction(leftKey.size() == 1 ? leftKey.get(0) : OperatorNode.create(PhysicalExprOperator.ARRAY, leftKey));

        OperatorNode<FunctionOperator> rightKeyFunction =
                function.createFunction(rightKey.size() == 1 ? rightKey.get(0) : OperatorNode.create(PhysicalExprOperator.ARRAY, rightKey));

        ExprScope joinOutputScope = new ExprScope();
        joinOutputScope.addArgument("$left");
        joinOutputScope.addArgument("$right");


        List<String> fieldNames = Lists.newArrayList();
        List<OperatorNode<PhysicalExprOperator>> fieldValues = Lists.newArrayList();
        mergeFields(leftRowType, fieldNames, fieldValues, OperatorNode.create(PhysicalExprOperator.LOCAL, "$left"));
        mergeFields(rightRowType, fieldNames, fieldValues, OperatorNode.create(PhysicalExprOperator.LOCAL, "$right"));

        OperatorNode<FunctionOperator> outputFunction =
                joinOutputScope.createFunction(OperatorNode.create(PhysicalExprOperator.RECORD, fieldNames, fieldValues));

        StreamValue leftSideJoin = StreamValue.iterate(this, leftSide);
        leftSideJoin.add(Location.NONE,
                operator == SequenceOperator.LEFT_JOIN ? StreamOperator.OUTER_HASH_JOIN : StreamOperator.HASH_JOIN,
                rightSide.materializeValue(),
                leftKeyFunction,
                rightKeyFunction,
                outputFunction);
        return leftSideJoin;
    }

    private void mergeFields(ReadFieldAliasAnnotate.RowType rowType, List<String> fieldNames, List<OperatorNode<PhysicalExprOperator>> fieldValues, OperatorNode<PhysicalExprOperator> row) {
        for(String alias : rowType.getAliases()) {
            fieldNames.add(alias);
            if(rowType.isJoin()) {
                fieldValues.add(OperatorNode.create(PhysicalExprOperator.PROPREF, row, alias));
            } else {
                fieldValues.add(row);
            }
        }

    }

    private StreamValue executeJoinRightQuery(OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> query) {
        OperatorNode<SequenceOperator> source = chainSource(query);
        switch (source.getOperator()) {
            case JOIN:
            case LEFT_JOIN:
            case MERGE:
            case PIPE:
            case EVALUATE:
            case EMPTY:
            case FALLBACK:
                return executeLocalJoin(leftSide, joinExpression, query);
            case SCAN:
                program.addStatement(CompiledProgram.ProgramStatement.SELECT);
                return executeReadJoin(leftSide, joinExpression, query, source);
            case INSERT:
            case DELETE:
            case DELETE_ALL:
            case UPDATE:
            case UPDATE_ALL:
                throw new ProgramCompileException(source.getLocation(), "%s may not be used as the right side of a JOIN", source.getOperator());
            case NEXT:
            case PAGE:
                throw new UnsupportedOperationException("NEXT not implemented");
            case MULTISOURCE:
                // this one should be rewritten out by the ExpandMultisource transform
            default:
                throw new ProgramCompileException("Unexpected query type to execute: %s", source.getOperator());
        }
    }

    StreamValue execute(OperatorNode<SequenceOperator> query) {
        OperatorNode<SequenceOperator> source = chainSource(query);
        switch (source.getOperator()) {
            case JOIN:
            case LEFT_JOIN:
            case MERGE:
            case PIPE:
            case EVALUATE:
            case EMPTY:
            case FALLBACK:
                return executeLocalChain(query);
            case SCAN:
                program.addStatement(CompiledProgram.ProgramStatement.SELECT);
                return executeRead(query, source);
            case INSERT:
                program.addStatement(CompiledProgram.ProgramStatement.INSERT);
                return executeWrite(query, source);
            case DELETE:
            case DELETE_ALL:
                program.addStatement(CompiledProgram.ProgramStatement.DELETE);
                return executeWrite(query, source);
            case UPDATE:
            case UPDATE_ALL:
                program.addStatement(CompiledProgram.ProgramStatement.UPDATE);
                return executeWrite(query, source);
            case NEXT:
            case PAGE:
                throw new UnsupportedOperationException("NEXT not yet implemented");
            case MULTISOURCE:
                // this one should be rewritten out by the ExpandMultisource transform
            default:
                throw new ProgramCompileException("Unexpected query type to execute: %s", source.getOperator());

        }
    }

    private StreamValue executeRead(OperatorNode<SequenceOperator> top, OperatorNode<SequenceOperator> source) {
        SourceType type = program.findSource(this, source);
        return type.plan(this, top, source);
    }

    private StreamValue executeReadJoin(OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> top, OperatorNode<SequenceOperator> source) {
        SourceType type = program.findSource(this, source);
        return type.join(this, leftSide, joinExpression, top, source);
    }

    private StreamValue executeWrite(OperatorNode<SequenceOperator> top, OperatorNode<SequenceOperator> write) {
        // {INSERT/UPDATE/UPDATE_ALL/DELETE/DELETE_ALL} SCAN ...
        OperatorNode<SequenceOperator> source = write.getArgument(0);
        Preconditions.checkArgument(source.getOperator() == SequenceOperator.SCAN, "Target of write is not SCAN?: %s ", source);
        SourceType type = program.findSource(this, source);
        return type.plan(this, top, source);
    }

    public List<OperatorNode<PhysicalExprOperator>> evaluateList(List<OperatorNode<ExpressionOperator>> args) {
        return eval.applyAll(args);
    }

    @Override
    public OperatorValue getVariable(String name) {
        return program.getVariable(name);
    }

    public ContextPlanner createContext(String... kv) {
        List<String> keys = Lists.newArrayListWithExpectedSize(kv.length / 2);
        List<OperatorNode<PhysicalExprOperator>> values = Lists.newArrayListWithExpectedSize(kv.length / 2);
        for (int i = 0; i < kv.length; i += 2) {
            keys.add(kv[i]);
            values.add(constant(kv[i + 1]));
        }
        OperatorValue newContext = OperatorStep.create(program.getValueTypeAdapter(), PhysicalOperator.EVALUATE, contextExpr,
                OperatorNode.create(PhysicalExprOperator.TRACE_CONTEXT,
                        OperatorNode.create(PhysicalExprOperator.RECORD,
                                keys,
                                values)
                )
        );
        return new ContextPlanner(program, newContext, eval);
    }


    public ContextPlanner create(String variableName) {
        return createContext("query", variableName);
    }

    public ContextPlanner timeout(OperatorNode<ExpressionOperator> timeoutMilliseconds) {
        OperatorValue newContext = OperatorStep.create(program.getValueTypeAdapter(), PhysicalOperator.EVALUATE,
                contextExpr,
                OperatorNode.create(PhysicalExprOperator.TIMEOUT_MAX, eval.apply(timeoutMilliseconds), eval.constant(TimeUnit.MILLISECONDS)));
        return new ContextPlanner(program, newContext, eval);
    }

    public ContextPlanner timeout(long minTimeoutMs, long maxTimeoutMs) {
        OperatorNode<PhysicalExprOperator> ms = eval.constant(TimeUnit.MILLISECONDS);
        OperatorValue newContext = OperatorStep.create(program.getValueTypeAdapter(), PhysicalOperator.EVALUATE,
                contextExpr,
                OperatorNode.create(PhysicalExprOperator.TIMEOUT_GUARD, minTimeoutMs, ms, maxTimeoutMs, ms));
        return new ContextPlanner(program, newContext, eval);
    }

    public OperatorValue end(OperatorValue value) {
        return OperatorStep.create(program.getValueTypeAdapter(), PhysicalOperator.EVALUATE, contextExpr,
                OperatorNode.create(PhysicalExprOperator.END_CONTEXT, OperatorNode.create(PhysicalExprOperator.VALUE, value)));
    }

    public StreamValue merge(List<StreamValue> outputs) {
        return StreamValue.merge(this, outputs);
    }

    public OperatorNode<PhysicalExprOperator> getContextExpr() {
        return contextExpr;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> constant(Object value) {
        return program.constant(value);
    }

    public ProgramValueTypeAdapter getValueTypeAdapter() {
        return program.getValueTypeAdapter();
    }

    public GambitScope getGambitScope() {
        return program.getGambitScope();
    }


    private StreamValue executeLocalChain(final OperatorNode<SequenceOperator> seq) {
        return new LocalPlanChain(this, seq).execute(seq);
    }

    private StreamValue executeLocalJoin(OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, final OperatorNode<SequenceOperator> seq) {
        return new LocalJoiningChain(this, seq, leftSide, joinExpression).execute(seq);
    }

    private static final EnumSet<SequenceOperator> CHAINABLE = EnumSet.of(SequenceOperator.PROJECT,
            SequenceOperator.EXTRACT,
            SequenceOperator.FILTER,
            SequenceOperator.SORT,
            SequenceOperator.OFFSET,
            SequenceOperator.LIMIT,
            SequenceOperator.SLICE,
            SequenceOperator.TIMEOUT);

    private OperatorNode<SequenceOperator> chainSource(OperatorNode<SequenceOperator> current) {
        if (CHAINABLE.contains(current.getOperator())) {
            // keep chaining transforms
            return chainSource((OperatorNode<SequenceOperator>) current.getArgument(0));
        }
        switch (current.getOperator()) {
            case JOIN:
            case LEFT_JOIN:
            case MERGE:
            case PIPE:
            case SCAN:
            case NEXT:
            case EMPTY:
            case EVALUATE:
            case FALLBACK:
            case MULTISOURCE:
            case PAGE:
            case INSERT:
            case DELETE:
            case DELETE_ALL:
            case UPDATE:
            case UPDATE_ALL:
                return current;
            default:
                throw new UnsupportedOperationException("Unknown operation type: " + current.getOperator());
        }
    }

    private OperatorNode<SequenceOperator> replaceTail(OperatorNode<SequenceOperator> current, OperatorNode<SequenceOperator> tail, OperatorNode<SequenceOperator> target) {
        if (CHAINABLE.contains(current.getOperator())) {
            // keep chaining transforms
            OperatorNode<SequenceOperator> next = replaceTail((OperatorNode<SequenceOperator>) current.getArgument(0), tail, target);

            Object[] args = current.getArguments();
            args[0] = next;
            return OperatorNode.create(current.getLocation(), current.getAnnotations(), current.getOperator(), args);
        }
        if (tail == current) {
            return target;
        } else {
            throw new IllegalStateException("Unexpected chain tail replacement failure (should never happen)");
        }
    }

    private class LocalJoiningChain extends PlanChain {
        private final OperatorNode<SequenceOperator> seq;
        private final OperatorNode<PhysicalExprOperator> leftSide;
        private final OperatorNode<ExpressionOperator> joinExpression;

        public LocalJoiningChain(ContextPlanner context, OperatorNode<SequenceOperator> seq, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression) {
            super(context);
            this.seq = seq;
            this.leftSide = leftSide;
            this.joinExpression = joinExpression;
        }

        @Override
        protected StreamValue executeSource(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> source) {
            switch (source.getOperator()) {
                case JOIN:
                case LEFT_JOIN: {
                    OperatorNode<SequenceOperator> leftQuery = source.getArgument(0);
                    OperatorNode<SequenceOperator> rightQuery = source.getArgument(1);
                    OperatorNode<ExpressionOperator> joinExpression = source.getArgument(2);
                    StreamValue leftSideStream = context.executeJoinRightQuery(this.leftSide, this.joinExpression, leftQuery);
                    OperatorNode<PhysicalExprOperator> leftSide = leftSideStream.materializeValue();
                    return executeJoin(source, leftSide, joinExpression, rightQuery);
                }
                case MERGE: {
                    List<OperatorNode<SequenceOperator>> inputs = source.getArgument(0);
                    List<StreamValue> inputStreams = Lists.newArrayList();
                    for (OperatorNode<SequenceOperator> input : inputs) {
                        inputStreams.add(context.executeJoinRightQuery(leftSide, joinExpression, input));
                    }
                    return StreamValue.merge(context, inputStreams);
                }
                case FALLBACK: {
                    OperatorNode<SequenceOperator> primary = source.getArgument(0);
                    OperatorNode<SequenceOperator> fallback = source.getArgument(1);
                    OperatorValue primarySeq = context.executeJoinRightQuery(leftSide, joinExpression, primary).materialize();
                    OperatorValue fallbackSeq = context.executeJoinRightQuery(leftSide, joinExpression, fallback).materialize();
                    OperatorNode<PhysicalExprOperator> tryCatch = OperatorNode.create(source.getLocation(), PhysicalExprOperator.CATCH,
                            OperatorNode.create(seq.getLocation(), PhysicalExprOperator.VALUE, primarySeq),
                            OperatorNode.create(seq.getLocation(), PhysicalExprOperator.VALUE, fallbackSeq));
                    return StreamValue.iterate(context, tryCatch);
                }
                case PIPE:
                case EVALUATE:
                case EMPTY: {
                    // no special processing for being the right side of a join
                    return context.execute(source);
                }

            }
            throw new ProgramCompileException(source.getLocation(), "Unexpected local chain source %s", source.getOperator());
        }
    }

    private static class LocalPlanChain extends PlanChain {
        private final OperatorNode<SequenceOperator> seq;

        public LocalPlanChain(ContextPlanner context, OperatorNode<SequenceOperator> seq) {
            super(context);
            this.seq = seq;
        }

        @Override
        protected StreamValue executeSource(ContextPlanner context, LocalChainState state, OperatorNode<SequenceOperator> source) {
            switch (source.getOperator()) {
                case JOIN:
                case LEFT_JOIN: {
                    OperatorNode<SequenceOperator> leftQuery = source.getArgument(0);
                    OperatorNode<SequenceOperator> rightQuery = source.getArgument(1);
                    OperatorNode<ExpressionOperator> joinExpression = source.getArgument(2);
                    StreamValue leftSideStream = context.execute(leftQuery);
                    OperatorNode<PhysicalExprOperator> leftSide = leftSideStream.materializeValue();
                    return context.executeJoin(source, leftSide, joinExpression, rightQuery);
                }
                case MERGE: {
                    List<OperatorNode<SequenceOperator>> inputs = source.getArgument(0);
                    List<StreamValue> inputStreams = Lists.newArrayList();
                    for (OperatorNode<SequenceOperator> input : inputs) {
                        inputStreams.add(context.execute(input));
                    }
                    return StreamValue.merge(context, inputStreams);
                }
                case FALLBACK: {
                    OperatorNode<SequenceOperator> primary = source.getArgument(0);
                    OperatorNode<SequenceOperator> fallback = source.getArgument(1);
                    OperatorValue primarySeq = context.execute(primary).materialize();
                    OperatorValue fallbackSeq = context.execute(fallback).materialize();
                    OperatorNode<PhysicalExprOperator> tryCatch = OperatorNode.create(source.getLocation(), PhysicalExprOperator.CATCH,
                            OperatorNode.create(seq.getLocation(), PhysicalExprOperator.VALUE, primarySeq),
                            OperatorNode.create(seq.getLocation(), PhysicalExprOperator.VALUE, fallbackSeq));
                    return StreamValue.iterate(context, tryCatch);
                }
                case PIPE: {
                    return context.executePipe(source);
                }
                case EVALUATE: {
                    context.program.addStatement(CompiledProgram.ProgramStatement.SELECT);
                    OperatorNode<ExpressionOperator> expr = source.getArgument(0);
                    return StreamValue.iterate(context, context.evaluate(expr));
                }
                case EMPTY: {
                    OperatorValue empty = context.constantValue(Sequences.singletonSequence());
                    return StreamValue.iterate(context, empty);
                }

            }
            throw new ProgramCompileException(source.getLocation(), "Unexpected local chain source %s", source.getOperator());
        }
    }
}

