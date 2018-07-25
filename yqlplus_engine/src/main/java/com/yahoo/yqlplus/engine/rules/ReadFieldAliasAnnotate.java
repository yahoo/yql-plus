/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.LogicalOperatorTransform;
import com.yahoo.yqlplus.language.logical.ProjectOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.SortOperator;
import com.yahoo.yqlplus.language.operator.Operator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.operator.OperatorVisitor;

import java.util.List;

/**
 * Annotate JOIN and LEFT_JOIN operations with the disposition of aliased records
 * Annotate READ_FIELD and READ_RECORD with the expected state of the row objects they operate on.
 */
public class ReadFieldAliasAnnotate extends LogicalOperatorTransform {
    public static class RowType {
        private static final String NAME = "rowType";
        private static final String LEFT_NAME = NAME + ".left";
        private static final String RIGHT_NAME = NAME + ".right";

        public static boolean hasAnnotation(OperatorNode<? extends Operator> target) {
            return target.getAnnotation(NAME) != null;
        }

        public static RowType getSequenceType(OperatorNode<SequenceOperator> target) {
            return (RowType)(target.getAnnotation(NAME));
        }

        public static RowType getLeftType(OperatorNode<SequenceOperator> target) {
            return (RowType)(target.getAnnotation(LEFT_NAME));
        }

        public static RowType getRightType(OperatorNode<SequenceOperator> target) {
            return (RowType)(target.getAnnotation(RIGHT_NAME));
        }

        public static RowType getReadRecordType(OperatorNode<? extends Operator> target) {
            return (RowType)(target.getAnnotation(NAME));
        }

        public static void setSequenceType(OperatorNode<SequenceOperator> target, RowType rowType) {
            target.putAnnotation(NAME, rowType);
        }

        public static void setLeftType(OperatorNode<SequenceOperator> target, RowType rowType) {
            target.putAnnotation(LEFT_NAME, rowType);
        }

        public static void setRightType(OperatorNode<SequenceOperator> target, RowType rowType) {
            target.putAnnotation(RIGHT_NAME, rowType);
        }

        public static void setReadRecordType(OperatorNode<? extends Operator> target, RowType rowType) {
            target.putAnnotation(NAME, rowType);
        }

        private final List<String> aliases;

        private RowType() {
            this.aliases = Lists.newArrayList();
        }

        public boolean isJoin() {
            return aliases.size() > 1;
        }

        public Iterable<String> getAliases() {
            return aliases;
        }

        void addAlias(String alias) {
            aliases.add(alias);
        }

        public void merge(RowType side) {
            for(String alias : side.aliases) {
                addAlias(alias);
            }
        }

        @Override
        public String toString() {
            return "[" + Joiner.on(",").join(aliases) + "]";
        }
    }

    private static class MergeRowType extends RowType {
        private RowType left;
        private RowType right;

        public MergeRowType(RowType left, RowType right) {
            this.left = left;
            this.right = right;
        }

        @Override
        void addAlias(String alias) {
            super.addAlias(alias);
            this.left.addAlias(alias);
            this.right.addAlias(alias);
        }
    }

    @Override
    public OperatorNode<SequenceOperator> visitSequenceOperator(OperatorNode<SequenceOperator> target) {
        // a JOIN output row will contain all of the aliases from both sides
        // a row can either
        //  be a JOIN output row (which will mean it is a record with a property for each join component)
        //  or a regular row (which will mean there is no 'wrapper' row)
        // this transform annotates the JOIN with the disposition of the input sides
        //    (indicating the type of each side and thus indicating if the JOIN output should MERGE or FIELD)
        // as well as annotating all record references (READ_FIELD, READ_RECORD, and ProjectOperator.MERGE)
        //    (indicating if they should expect a JOIN output row as input or not)


        // first, visit the children to figure out what type of input this is operating on
        if(RowType.hasAnnotation(target)) {
           return target;
        }
        RowType rowType = visitForRowType(target);
        if(target.getAnnotation("alias") != null) {
            rowType.addAlias((String) target.getAnnotation("alias"));
        }
        RowType.setSequenceType(target, rowType);
        visitForReadRecord(target);
        return target;
    }

    private void visitForReadRecord(OperatorNode<SequenceOperator> target) {
        switch(target.getOperator()) {
            case SCAN:
            case INSERT:
            case UPDATE:
            case UPDATE_ALL:
            case DELETE:
            case DELETE_ALL:
            case EMPTY:
            case EVALUATE:
            case PIPE:
            case DISTINCT:
            case LIMIT:
            case OFFSET:
            case SLICE:
            case MERGE:
            case TIMEOUT:
            case ALL:
            case MULTISOURCE:
            case FALLBACK:
                // no row-level inputs
                return;
            case PROJECT: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                RowTypeVisitor rowTypeVisitor = new RowTypeVisitor(RowType.getSequenceType(input));
                List<OperatorNode<ProjectOperator>> projection = target.getArgument(1);
                for(OperatorNode<ProjectOperator> op : projection) {
                    switch(op.getOperator()) {
                        case FIELD: {
                            OperatorNode<ExpressionOperator> expr = op.getArgument(0);
                            expr.visit(rowTypeVisitor);
                            break;
                        }
                        case MERGE_RECORD: {
                            RowType.setReadRecordType(op, RowType.getSequenceType(input));
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException("Unknown ProjectOperator: " + op);
                    }
                }
                break;
            }
            case FILTER:
            case EXTRACT: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                RowTypeVisitor rowTypeVisitor = new RowTypeVisitor(RowType.getSequenceType(input));
                OperatorNode<ExpressionOperator> expr = target.getArgument(1);
                expr.visit(rowTypeVisitor);
                break;
            }
            case SORT: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                RowTypeVisitor rowTypeVisitor = new RowTypeVisitor(RowType.getSequenceType(input));
                List<OperatorNode<SortOperator>> comparator = target.getArgument(1);
                for(OperatorNode<SortOperator> op : comparator) {
                    OperatorNode<ExpressionOperator> order = op.getArgument(0);
                    order.visit(rowTypeVisitor);
                }
                break;
            }
            case JOIN:
            case LEFT_JOIN: {
                // left right joinCondition
                OperatorNode<ExpressionOperator> joinCondition = target.getArgument(2);
                RowType leftSide = RowType.getLeftType(target);
                RowType rightSide = RowType.getRightType(target);
                RowTypeVisitor leftVisitor = new RowTypeVisitor(leftSide);
                RowTypeVisitor rightVisitor = new RowTypeVisitor(rightSide);
                for(JoinExpression expr : JoinExpression.parse(joinCondition)) {
                    expr.left.visit(leftVisitor);
                    expr.right.visit(rightVisitor);
                }
                break;
            }
            default:
                throw new UnsupportedOperationException("Unknown SequenceOperator: %s" + target);
        }
    }

    private static class RowTypeVisitor implements OperatorVisitor {
        private final RowType rowType;
        public RowTypeVisitor(RowType rowType) {
            Preconditions.checkNotNull(rowType);
            this.rowType = rowType;
        }

        @Override
        public <T extends Operator> boolean enter(OperatorNode<T> node) {
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Operator> void exit(OperatorNode<T> node) {
            if(ExpressionOperator.IS.apply(node)) {
                OperatorNode<ExpressionOperator> expr = (OperatorNode<ExpressionOperator>) node;
                switch(expr.getOperator()) {
                    case AND:
                    case OR:
                    case EQ:
                    case NEQ:
                    case LT:
                    case GT:
                    case LTEQ:
                    case GTEQ:
                    case IN:
                    case NOT_IN:
                    case LIKE:
                    case NOT_LIKE:
                    case IS_NULL:
                    case IS_NOT_NULL:
                    case MATCHES:
                    case NOT_MATCHES:
                    case CONTAINS:
                    case ADD:
                    case SUB:
                    case MULT:
                    case DIV:
                    case MOD:
                    case NEGATE:
                    case NOT:
                    case MAP:
                    case ARRAY:
                    case INDEX:
                    case PROPREF:
                    case CALL:
                    case VARREF:
                    case LITERAL:
                    case READ_MODULE:
                    case NULL:
                        return;
                    case READ_RECORD:
                    case READ_FIELD: {
                        RowType.setReadRecordType(expr, rowType);
                        break;
                    }
                    case IN_QUERY:
                    case NOT_IN_QUERY:
                        throw new UnsupportedOperationException("Unexpected operation (should be rewritten into IN by prior transform): " + expr);
                    default:
                        throw new UnsupportedOperationException("Unexpected operation (unknown): " + expr);
                }
            }
        }
    }

    private RowType visitForRowType(OperatorNode<SequenceOperator> target) {
        switch(target.getOperator()) {
            case SCAN:
            case UPDATE:
            case UPDATE_ALL:
            case DELETE:
            case DELETE_ALL:
            case EMPTY:
            case EVALUATE:
            case ALL:
            case MULTISOURCE:
                return new RowType();
            case MERGE: {
                List<OperatorNode<SequenceOperator>> inputs = target.getArgument(0);
                for(OperatorNode<SequenceOperator> input : inputs) {
                    visitSequenceOperator(input);
                }
                return new RowType();
            }
            case PIPE: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                visitSequenceOperator(input);
                return new RowType();
            }
            case INSERT: {
                OperatorNode<SequenceOperator> input = target.getArgument(1);
                visitSequenceOperator(input);
                return new RowType();
            }
            case PROJECT: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                visitSequenceOperator(input);
                return new RowType();
            }
            case DISTINCT:
            case EXTRACT:
            case FILTER:
            case SORT:
            case LIMIT:
            case OFFSET:
            case TIMEOUT:
            case SLICE: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                input = visitSequenceOperator(input);
                return RowType.getSequenceType(input);
            }
            case FALLBACK: {
                OperatorNode<SequenceOperator> input = target.getArgument(0);
                input = visitSequenceOperator(input);
                OperatorNode<SequenceOperator> otherInput = target.getArgument(1);
                otherInput = visitSequenceOperator(otherInput);
                return new MergeRowType(RowType.getSequenceType(input), RowType.getSequenceType(otherInput));
            }
            case JOIN:
            case LEFT_JOIN: {
                OperatorNode<SequenceOperator> left = target.getArgument(0);
                OperatorNode<SequenceOperator> right = target.getArgument(1);
                visitSequenceOperator(left);
                visitSequenceOperator(right);
                RowType leftSide = RowType.getSequenceType(left);
                RowType rightSide = RowType.getSequenceType(right);
                RowType.setLeftType(target, leftSide);
                RowType.setRightType(target, rightSide);
                RowType joinOutputType = new RowType();
                joinOutputType.merge(leftSide);
                joinOutputType.merge(rightSide);
                return joinOutputType;
            }
            default:
                throw new UnsupportedOperationException("Unknown SequenceOperator: %s" + target);
        }
    }


}