package com.yahoo.yqlplus.engine.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.yahoo.cloud.metrics.api.MetricEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.annotations.*;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.index.IndexColumn;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.ChainState;
import com.yahoo.yqlplus.engine.CompileContext;
import com.yahoo.yqlplus.engine.SourceType;
import com.yahoo.yqlplus.engine.StreamValue;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.compiler.code.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.compiler.code.NotNullableTypeWidget;
import com.yahoo.yqlplus.engine.compiler.code.PropertyAdapter;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.indexed.*;
import com.yahoo.yqlplus.engine.rules.JoinExpression;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.*;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.yahoo.yqlplus.engine.source.ExportModuleAdapter.isFreeArgument;

public class SourceAdapter implements SourceType {
    private final String sourceName;
    private final Class<?> clazz;
    private final Supplier<?> supplier;
    private OperatorNode<PhysicalExprOperator> source;

    public SourceAdapter(String sourceName, Class<?> clazz, Supplier<?> module) {
        this.sourceName = sourceName;
        this.clazz = clazz;
        this.supplier = module;
    }

    public SourceAdapter(String sourceName, Class<?> clazz) {
        this.sourceName = sourceName;
        this.clazz = clazz;
        this.supplier = null;
    }

    @Override
    public StreamValue plan(CompileContext planner, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        List<OperatorNode<PhysicalExprOperator>> argsExprs = planner.evaluateList(args);
        List<OperatorNode<PhysicalExprOperator>> argsEvaled = planner.computeExprs(argsExprs);
        return planner.executeSource(query, (ctx, st, q) -> executeSource(argsEvaled, ctx, st, q));
    }

    @Override
    public StreamValue join(CompileContext planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> right, OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        List<OperatorNode<PhysicalExprOperator>> argsExprs = planner.evaluateList(args);
        List<OperatorNode<PhysicalExprOperator>> argsEvaled = planner.computeExprs(argsExprs);
        return planner.executeSource(right, (ctx, st, q) -> executeSourceJoin(leftSide, joinExpression, argsEvaled, ctx, st, q));
    }

    private StreamValue executeSourceJoin(OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, ChainState state, OperatorNode<SequenceOperator> query) {
        switch(query.getOperator()) {
            case SCAN:
                return executeSelect(query, leftSide, joinExpression, args, context, state);
            case INSERT:
            case UPDATE:
            case UPDATE_ALL:
            case DELETE:
            case DELETE_ALL:
            default:
                throw new ProgramCompileException(query.getLocation(), "Operator %s is not supported on right side of join", query.getOperator());
        }
    }
    private StreamValue executeSource(List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, ChainState state, OperatorNode<SequenceOperator> query) {
        switch(query.getOperator()) {
            case SCAN:
                return executeSelect(query, args, context, state);
            case INSERT: {
                OperatorNode<SequenceOperator> records = query.getArgument(1);
                List<Map<String, OperatorNode<PhysicalExprOperator>>> writeRecords = decodeInsertSequenceMaps(context, records);
                if(writeRecords != null) {
                    return executeInsertSet(query, writeRecords, args, context, state);
                }
                return executeInsertStream(query, context.execute(records), args, context, state);
            }
            case UPDATE: {
                OperatorNode<ExpressionOperator> record = query.getArgument(1);
                Map<String, OperatorNode<PhysicalExprOperator>> recordFields = decodeExpressionMap(context, record);
                OperatorNode<ExpressionOperator> filter = query.getArgument(2);
                return executeUpdate(query, args, context, recordFields, filter);
            }
            case UPDATE_ALL: {
                OperatorNode<ExpressionOperator> record = query.getArgument(1);
                Map<String, OperatorNode<PhysicalExprOperator>> recordFields = decodeExpressionMap(context, record);
                return executeUpdateAll(query, args, context, recordFields);
            }
            case DELETE: {
                OperatorNode<ExpressionOperator> filter = query.getArgument(1);
                return executeDelete(query, args, context, filter);
            }
            case DELETE_ALL:
                return executeDeleteAll(query, args, context);
            default:
                throw new UnsupportedOperationException();
        }
    }


    private StreamValue executeDelete(OperatorNode<SequenceOperator> query, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, OperatorNode<ExpressionOperator> filter) {
        List<QM> candidates = visitMethods(Delete.class, context, args, new Visitor() {
            @Override
            public boolean visitKey(QM qm, Key key, Class<?> parameterClazz, TypeWidget parameterType) {
                qm.addKeyArgument(key, parameterClazz, parameterType);
                return true;
            }
        });
        return executeIndexWrite(Delete.class, query, context, filter, candidates);
    }

    private StreamValue executeIndexWrite(Class<? extends Annotation> annotation, OperatorNode<SequenceOperator> query, CompileContext context, OperatorNode<ExpressionOperator> filter, List<QM> candidates) {
        if(candidates.isEmpty()) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has no matching method for %s (e.g. @%s methods with matching @Key parameters)", sourceName, query.toString(), annotation.getSimpleName());
        } else {
            List<IndexDescriptor> descriptors = Lists.newArrayList();
            Map<IndexKey, QM> methodMap = Maps.newLinkedHashMap();
            for(QM m : candidates) {
                if(!m.keyArguments.isEmpty()) {
                    IndexDescriptor desc = m.indexBuilder.build();
                    methodMap.put(IndexKey.of(desc.getColumnNames()), m);
                    descriptors.add(desc);
                }
            }
            IndexedQueryPlanner indexPlanner = new IndexedQueryPlanner(descriptors);
            QueryStrategy qs = indexPlanner.planExact(filter);
            if (qs.scan) {
                throw new ProgramCompileException(query.getLocation(), "%s must exactly match indexes (e.g. @%s methods w/ @Key parameters)", query.getOperator(), annotation.getSimpleName());
            }

            List<StreamValue> outputs = Lists.newArrayList();
            for (IndexKey index : qs.indexes.keySet()) {
                Collection<IndexStrategy> strategyCollection = qs.indexes.get(index);
                Multimap<IndexKey, IndexQuery> split = ArrayListMultimap.create();
                for (IndexStrategy strategy : strategyCollection) {
                    Preconditions.checkState(strategy.filter == null, "Internal error: index strategy planner for DELETE must use exact index matches");
                    IndexQuery iq = new IndexQuery();
                    prepareIndexKeyValues(context, null, null, strategy, iq);
                    iq.index = index;
                    iq.handledFilter = true;
                    split.put(iq.index, iq);
                }
                for (IndexKey idx : split.keySet()) {
                    Collection<IndexQuery> todo = split.get(idx);
                    methodMap.get(idx).index(outputs, context, Lists.newArrayList(todo));
                }
            }
            if (outputs.isEmpty()) {
                throw new ProgramCompileException("Unable to match execution strategy for query %s (? should not be reached given declared indexes)", query);
            }
            StreamValue result;
            if (outputs.size() == 1) {
                result = outputs.get(0);
            } else {
                result = StreamValue.merge(context, outputs);
            }
            return result;
        }
    }

    private StreamValue executeDeleteAll(OperatorNode<SequenceOperator> query, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context) {
        List<QM> candidates = visitMethods(Delete.class, context, args, new Visitor() {
            @Override
            public boolean visitKey(QM qm, Key key, Class<?> parameterClazz, TypeWidget setType) {
                // delete all does not consider any methods with filter arguments
                return false;

            }
        });
        if(candidates.isEmpty()) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has no matching @Delete method for %s", sourceName, query.toString());
        } else if (candidates.size() > 1) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has too many matching @Delete methods for %s (%d)", sourceName, query.toString(), candidates.size());
        } else {
            return candidates.get(0).stream(context);
        }
    }

    private StreamValue executeUpdateAll(OperatorNode<SequenceOperator> query, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, Map<String, OperatorNode<PhysicalExprOperator>> record) {
        List<QM> candidates = visitMethods(Update.class, context, args, new UpdateVisitor(record, context));
        if(candidates.isEmpty()) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has no matching @Update method for %s", sourceName, query.toString());
        } else if (candidates.size() > 1) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has too many matching @Update methods for %s (%d)", sourceName, query.toString(), candidates.size());
        } else {
            candidates.get(0).verifySetArguments(record);
            return candidates.get(0).stream(context);
        }
    }

    private StreamValue executeUpdate(OperatorNode<SequenceOperator> query, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, Map<String, OperatorNode<PhysicalExprOperator>> record, OperatorNode<ExpressionOperator> filter) {
        List<QM> candidates = visitMethods(Update.class, context, args, new UpdateVisitor(record, context) {
            @Override
            public boolean visitKey(QM qm, Key key, Class<?> parameterClazz, TypeWidget parameterType) {
                qm.addKeyArgument(key, parameterClazz, parameterType);
                return true;
            }
        });
        candidates.sort((l, r) -> (-Integer.compare(l.checkSetArguments(record), r.checkSetArguments(record))));
        if(!candidates.isEmpty()) {
            candidates.get(0).verifySetArguments(record);
        }
        return executeIndexWrite(Update.class, query, context, filter, candidates);
    }

    private StreamValue executeInsertStream(OperatorNode<SequenceOperator> query, StreamValue records, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, ChainState state) {
        ExprScope function = new ExprScope();
        OperatorNode<PhysicalExprOperator> row = function.addArgument("$row");
        List<QM> candidates = visitMethods(Insert.class, context, args, new Visitor() {
            @Override
            public boolean visitSet(QM qm, String keyName, Object defaultValue, Class<?> parameterType, TypeWidget setType) {
                if(defaultValue != null) {
                    qm.addSetParameter(keyName, OperatorNode.create(PhysicalExprOperator.PROPREF_DEFAULT, row, keyName, OperatorNode.create(PhysicalExprOperator.CONSTANT, setType, defaultValue)));
                } else {
                    qm.addSetParameter(keyName, OperatorNode.create(PhysicalExprOperator.PROPREF_DEFAULT, row, keyName, OperatorNode.create(PhysicalExprOperator.THROW,
                            OperatorNode.create(PhysicalExprOperator.INVOKENEW, PropertyNotFoundException.class, ImmutableList.of(context.constant(String.format("Required property '%s' not found on INSERT", keyName)))))));
                }
                return true;
            }
        });
        if(candidates.isEmpty()) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has no matching @Insert method for %s", sourceName, query.toString());
        } else if (candidates.size() > 1) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has too many matching @Insert method for %s (%d)", sourceName, query.toString(), candidates.size());
        } else {
            QM m = candidates.get(0);
            OperatorNode<FunctionOperator> func = function.createFunction(m.invoke());
            records.add(func.getLocation(), StreamOperator.TRANSFORM, func);
            if(!m.singleton) {
                records.add(func.getLocation(), StreamOperator.FLATTEN);
            }
            return records;
        }
    }

    private StreamValue executeInsertSet(OperatorNode<SequenceOperator> query, List<Map<String, OperatorNode<PhysicalExprOperator>>> records, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, ChainState state) {
        List<QM> invocations = Lists.newArrayList();
        for(Map<String, OperatorNode<PhysicalExprOperator>> record : records) {
            List<QM> candidates = visitMethods(Insert.class, context, args, new RecordWriteVisitor(record, context));
            if(candidates.isEmpty()) {
                throw new ProgramCompileException(query.getLocation(), "Source '%s' has no matching @Insert method for %s", sourceName, query.toString());
            } else {
                candidates.sort((l, r) -> (-Integer.compare(l.checkSetArguments(record), r.checkSetArguments(record))));
                candidates.get(0).verifySetArguments(record);
                invocations.add(candidates.get(0));
            }
        }
        if(invocations.size() == 1) {
            return invocations.get(0).stream(context);
        } else {
            boolean allSingleton = invocations.stream().allMatch((c) -> c.singleton);
            boolean allFlatten = invocations.stream().noneMatch((c) -> c.singleton);
            if(allSingleton || allFlatten) {
                List<OperatorNode<PhysicalExprOperator>> exprs = invocations.stream().map(QM::invoke).collect(Collectors.toList());
                OperatorNode<PhysicalExprOperator> list = OperatorNode.create(PhysicalExprOperator.ARRAY, exprs);
                StreamValue val = StreamValue.iterate(context, list);
                val.add(Location.NONE, StreamOperator.RESOLVE);
                if(allFlatten) {
                    val.add(Location.NONE, StreamOperator.FLATTEN);
                }
                return val;
            } else {
                List<OperatorNode<PhysicalExprOperator>> exprs = invocations.stream().map(QM::invokeIterable).collect(Collectors.toList());
                OperatorNode<PhysicalExprOperator> list = OperatorNode.create(PhysicalExprOperator.ARRAY, exprs);
                StreamValue val = StreamValue.iterate(context, list);
                val.add(Location.NONE, StreamOperator.RESOLVE);
                val.add(Location.NONE, StreamOperator.FLATTEN);
                return val;
            }
        }

    }

    private StreamValue executeSelect(OperatorNode<SequenceOperator> query, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, ChainState state) {
        return executeSelect(query,null, null, args, context, state);
    }

    private StreamValue executeSelect(OperatorNode<SequenceOperator> query, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, List<OperatorNode<PhysicalExprOperator>> args, CompileContext context, ChainState state) {
        List<QM> candidates = visitMethods(Query.class, context, args, new Visitor() {
            @Override
            public boolean visitKey(QM qm, Key key, Class<?> parameterClazz, TypeWidget parameterType) {
                qm.addKeyArgument(key, parameterClazz, parameterType);
                return true;
            }
        });
        if(candidates.isEmpty()) {
            throw new ProgramCompileException(query.getLocation(), "Source '%s' has no matching @Query method for %s", sourceName, query.toString());
        } else {
            List<IndexDescriptor> descriptors = Lists.newArrayList();
            Map<IndexKey, QM> methodMap = Maps.newLinkedHashMap();
            QM scan = null;
            for(QM m : candidates) {
                if(!m.keyArguments.isEmpty()) {
                    IndexDescriptor desc = m.indexBuilder.build();
                    methodMap.put(IndexKey.of(desc.getColumnNames()), m);
                    descriptors.add(desc);
                } else {
                    scan = m;
                }
            }
            IndexedQueryPlanner indexPlanner = new IndexedQueryPlanner(descriptors);
            QueryStrategy qs = leftSide != null ?
                    indexPlanner.planJoin(leftSide, joinExpression, state.getFilter())
                    : indexPlanner.plan(state.getFilter());
            if (qs.scan) {
                if (scan == null) {
                    throw new ProgramCompileException(query.getLocation(), "Source '%s' does not support SCAN and query has no matching index", sourceName);
                } else {
                    return scan.stream(context);
                }
            }

            List<StreamValue> outputs = Lists.newArrayList();
            boolean handledFilter = true;
            for (IndexKey index : qs.indexes.keySet()) {
                Collection<IndexStrategy> strategyCollection = qs.indexes.get(index);
                Multimap<IndexKey, IndexQuery> split = ArrayListMultimap.create();
                for (IndexStrategy strategy : strategyCollection) {
                    IndexQuery iq = new IndexQuery();
                    prepareIndexKeyValues(context, leftSide, joinExpression, strategy, iq);
                    iq.index = index;
                    iq.filter = strategy.filter;
                    if (strategy.filter != null) {
                        iq.filterPredicate = compileFilter(context, strategy.filter);
                        iq.handledFilter = false;
                    } else {
                        iq.handledFilter = true;
                    }
                    split.put(iq.index, iq);
                }
                for (IndexKey idx : split.keySet()) {
                    Collection<IndexQuery> todo = split.get(idx);
                    methodMap.get(idx).index(outputs, context, Lists.newArrayList(todo));
                }
                for (IndexQuery iq : split.values()) {
                    handledFilter = handledFilter && iq.handledFilter;
                }
            }
            if (outputs.isEmpty()) {
                throw new ProgramCompileException("Unable to match execution strategy for query %s (? should not be reached given declared indexes)", query);
            }
            StreamValue result;
            if (outputs.size() == 1) {
                result = outputs.get(0);
            } else {
                result = StreamValue.merge(context, outputs);
            }
            if (handledFilter) {
                state.setFilterHandled(true);
            }
            return result;
        }
    }

    protected OperatorNode<FunctionOperator> compileFilter(CompileContext context, OperatorNode<ExpressionOperator> filter) {
        ExprScope scoper = new ExprScope();
        OperatorNode<PhysicalExprOperator> item = scoper.addArgument("item");
        OperatorNode<PhysicalExprOperator> predicate = context.evaluateInRowContext(filter, item);
        predicate = OperatorNode.create(filter.getLocation(), PhysicalExprOperator.BOOL, predicate);
        return scoper.createFunction(predicate);
    }


    private void prepareIndexKeyValues(CompileContext context,OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, IndexStrategy strategy, IndexQuery iq) {
        if(strategy.indexFilter != null) {
            for (Map.Entry<String, OperatorNode<ExpressionOperator>> e : strategy.indexFilter.entrySet()) {
                String key = e.getKey();
                OperatorNode<ExpressionOperator> zip = e.getValue();
                OperatorNode<PhysicalExprOperator> keyExpr = context.evaluate(zip.getArgument(1));
                switch (zip.getOperator()) {
                    case EQ:
                        keyExpr = OperatorNode.create(keyExpr.getLocation(), PhysicalExprOperator.ARRAY, ImmutableList.of(keyExpr));
                        break;
                    case IN:
                        break;
                    default:
                        throw new ProgramCompileException("unknown operator: %s", zip);
                }
                com.yahoo.yqlplus.api.index.IndexColumn column = strategy.descriptor.getColumn(key);
                keyExpr = filterKeyArray(column, keyExpr);
                iq.keyValues.put(key, keyExpr);
            }
        }
        if(strategy.joinColumns != null && !strategy.joinColumns.isEmpty()) {
            if(leftSide == null || joinExpression == null) {
                throw new NullPointerException("joinColumns is non-empty yet there is no available leftSide");
            }
            // generate an expression to extract the relevant keys from the left side
            List<JoinExpression> join = JoinExpression.parse(joinExpression);
            List<String> fields = Lists.newArrayListWithExpectedSize(join.size());
            List<OperatorNode<PhysicalExprOperator>> expressions = Lists.newArrayListWithExpectedSize(join.size());
            ExprScope scope = new ExprScope();
            scope.addArgument("$row");
            final OperatorNode<PhysicalExprOperator> rowReference = OperatorNode.create(PhysicalExprOperator.LOCAL, "$row");
            List<OperatorNode<PhysicalExprOperator>> nullTests = Lists.newArrayListWithExpectedSize(join.size());
            for(JoinExpression expr : join) {
                String rightField = expr.getRightField();
                if(strategy.joinColumns.contains(rightField)) {
                    fields.add(rightField);
                    expressions.add(context.evaluateInRowContext(expr.left, rowReference));
                    final OperatorNode<PhysicalExprOperator> fieldValue = OperatorNode.create(PhysicalExprOperator.PROPREF, rowReference, rightField);
                    com.yahoo.yqlplus.api.index.IndexColumn column = strategy.descriptor.getColumn(rightField);
                    if(column.isSkipNull() || column.isSkipEmpty()) {
                        nullTests.add(OperatorNode.create(PhysicalExprOperator.IS_NULL,
                                fieldValue));
                    }
                    if(column.isSkipEmpty()) {
                        addEmptyTest(nullTests, column, fieldValue);
                    }
                }
            }
            OperatorNode<FunctionOperator> keyFunction =
                    scope.createFunction(OperatorNode.create(PhysicalExprOperator.RECORD, fields, expressions));
            StreamValue leftKeys = StreamValue.iterate(context, leftSide);
            leftKeys.add(Location.NONE, StreamOperator.TRANSFORM, keyFunction);
            if(!nullTests.isEmpty()) {
                OperatorNode<PhysicalExprOperator> anyNull = nullTests.size() == 1 ? nullTests.get(0) : OperatorNode.create(PhysicalExprOperator.OR, nullTests);
                OperatorNode<FunctionOperator> filterFunction =
                        scope.createFunction(OperatorNode.create(PhysicalExprOperator.NOT, anyNull));
                leftKeys.add(Location.NONE, StreamOperator.FILTER, filterFunction);
            }
            leftKeys.add(Location.NONE, StreamOperator.DISTINCT);
            iq.joinKeyStream = leftKeys;
            iq.joinKeys = fields;
        }
    }

    private OperatorNode<PhysicalExprOperator> filterKeyArray(IndexColumn column, OperatorNode<PhysicalExprOperator> keyExpr) {
        ExprScope scope = new ExprScope();
        final OperatorNode<PhysicalExprOperator> keyReference = scope.addArgument("$key");;
        List<OperatorNode<PhysicalExprOperator>> tests = Lists.newArrayList();
        if(column.isSkipNull() || column.isSkipEmpty()) {
            tests.add(OperatorNode.create(PhysicalExprOperator.IS_NULL, keyReference));
        }
        if(column.isSkipEmpty()) {
            addEmptyTest(tests, column, keyReference);
        }
        if(tests.isEmpty()) {
            return keyExpr;
        }
        OperatorNode<PhysicalExprOperator> test;
        if(tests.size() == 1) {
            test = tests.get(0);
        } else {
            test = OperatorNode.create(PhysicalExprOperator.OR, tests);
        }
        OperatorNode<FunctionOperator> filter = scope.createFunction(OperatorNode.create(PhysicalExprOperator.NOT, test));
        return OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE,
                keyExpr,
                OperatorNode.create(StreamOperator.FILTER,
                        OperatorNode.create(StreamOperator.SINK, OperatorNode.create(SinkOperator.ACCUMULATE)),
                        filter));
    }

    private void addEmptyTest(List<OperatorNode<PhysicalExprOperator>> emptyTests, IndexColumn column, OperatorNode<PhysicalExprOperator> fieldValue) {
        switch(column.getType().getCoreType()) {
            case INT32:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.INT32, 0)));
                break;
            case INT64:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.INT64, 0L)));
                break;
            case FLOAT32:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.FLOAT32, 0.0f)));
                break;
            case FLOAT64:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, BaseTypeAdapter.FLOAT64, 0.0)));
                break;
            case STRING:
                emptyTests.add(OperatorNode.create(PhysicalExprOperator.EQ, fieldValue, OperatorNode.create(PhysicalExprOperator.CONSTANT, NotNullableTypeWidget.create(BaseTypeAdapter.STRING), "")));
                break;
        }
    }


    private Map<String, OperatorNode<PhysicalExprOperator>> decodeExpressionMap(CompileContext context, OperatorNode<ExpressionOperator> map) {
        Preconditions.checkArgument(map.getOperator() == ExpressionOperator.MAP);
        List<String> names = map.getArgument(0);
        List<OperatorNode<ExpressionOperator>> values = map.getArgument(1);
        List<OperatorNode<PhysicalExprOperator>> valueExprs = context.evaluateList(values);
        TreeMap<String, OperatorNode<PhysicalExprOperator>> b = new TreeMap<>(
                String.CASE_INSENSITIVE_ORDER);
        for(int i = 0; i < names.size(); i++) {
            b.put(names.get(i), valueExprs.get(i));
        }
        return b;
    }

    private List<Map<String, OperatorNode<PhysicalExprOperator>>> decodeInsertSequenceMaps(CompileContext context, OperatorNode<SequenceOperator> records) {
        if(records.getOperator() == SequenceOperator.EVALUATE) {
            OperatorNode<ExpressionOperator> expr = records.getArgument(0);
            if(expr.getOperator() == ExpressionOperator.ARRAY) {
                List<OperatorNode<ExpressionOperator>> maps = expr.getArgument(0);
                List<Map<String, OperatorNode<PhysicalExprOperator>>> output = Lists.newArrayListWithExpectedSize(maps.size());
                for(OperatorNode<ExpressionOperator> op : maps) {
                    if(op.getOperator() == ExpressionOperator.MAP) {
                        output.add(decodeExpressionMap(context, op));
                    } else {
                        return null;
                    }
                }
                return output;
            }
        }
        return null;
    }

    interface Adapter {
        TypeWidget adapt(java.lang.reflect.Type type, boolean nullable);
    }

    interface Visitor {
        default boolean visitSet(QM qm, String keyName, Object defaultValue, Class<?> parameterType, TypeWidget setType) {
            reportMethodException(qm.method, "@%s methods may not have @Set parameters", qm.methodType);
            return false;
        }

        default boolean visitKey(QM qm, Key key, Class<?> parameterClazz, TypeWidget setType) {
            reportMethodException(qm.method, "@%s methods may not have @Key parameters", qm.methodType);
            return false;
        }
    }

    private List<QM> visitMethods(Class<? extends Annotation> annotationClass, CompileContext types, List<OperatorNode<PhysicalExprOperator>> inputArgs, Visitor visitor) {
        List<QM> candidates = Lists.newArrayList();
        OperatorNode<PhysicalExprOperator> sourceExpr = getSource(types);
        methods:
        for(Method method : clazz.getMethods()) {
            if(!Modifier.isPublic(method.getModifiers()) || method.getAnnotation(annotationClass) == null) {
                continue;
            }
            Class<?>[] argumentTypes = method.getParameterTypes();
            java.lang.reflect.Type[] genericArgumentTypes = method.getGenericParameterTypes();
            Annotation[][] annotations = method.getParameterAnnotations();
            Iterator<OperatorNode<PhysicalExprOperator>> inputNext = inputArgs.iterator();
            TypeWidget outputType = types.adapt(method.getGenericReturnType(), true);
            TypeWidget rowType = outputType;
            boolean singleton = true;
            if (outputType.isPromise()) {
                rowType = outputType.getPromiseAdapter().getResultType();
            }
            if (rowType.isIterable()) {
                singleton = false;
                rowType = rowType.getIterableAdapter().getValue();
            }
            OperatorNode<PhysicalExprOperator> dimensions = OperatorNode.create(PhysicalExprOperator.RECORD,
                    ImmutableList.of("source", "method"),
                    ImmutableList.of(types.constant(sourceName), types.constant(method.getName())));
            QM m = new QM(annotationClass, method, sourceExpr, singleton, rowType, dimensions);

            if (!rowType.hasProperties()) {
                throw new YQLTypeException("Source method " + method + " does not return a STRUCT type: " + rowType);
            }
            for (int i = 0; i < argumentTypes.length; ++i) {
                Class<?> parameterType = argumentTypes[i];
                java.lang.reflect.Type genericType = genericArgumentTypes[i];
                if (isFreeArgument(argumentTypes[i], annotations[i])) {
                    if (!inputNext.hasNext()) {
                        continue methods;
                    }
                    m.invokeArguments.add(inputNext.next());
                } else {
                    for (Annotation annotate : annotations[i]) {
                        if (annotate instanceof Key) {
                            Key key = (Key) annotate;
                            if(Insert.class.isAssignableFrom(annotationClass)) {
                                reportMethodParameterException("Insert", method, "@Key parameters are not permitted on @Insert methods");
                                throw new IllegalArgumentException();
                            }
                            if(!visitor.visitKey(m, key, parameterType, types.adapt(genericType, true))) {
                                continue methods;
                            }
                        } else if (annotate instanceof Set) {
                            Object defaultValue = null;
                            Set set = (Set) annotate;
                            TypeWidget setType = types.adapt(genericArgumentTypes[i], true);
                            for (Annotation ann : annotations[i]) {
                                if (ann instanceof DefaultValue) {
                                    defaultValue = parseDefaultValue(method, set.value(), setType, ((DefaultValue) ann).value());
                                }
                            }
                            if(!Insert.class.isAssignableFrom(annotationClass) && !Update.class.isAssignableFrom(annotationClass)) {
                                reportMethodParameterException(annotationClass.getSimpleName(), method, "@Set parameters are only permitted on @Insert and @Update methods");
                                throw new IllegalArgumentException();
                            }
                            if(!visitor.visitSet(m, set.value(), defaultValue, parameterType, setType)) {
                                continue methods;
                            }
                        } else if (annotate instanceof TimeoutMilliseconds) {
                            if (!Long.TYPE.isAssignableFrom(parameterType)) {
                                reportMethodParameterException("TimeoutMilliseconds", method, "@TimeoutMilliseconds argument type must be a primitive long");
                            }
                            m.invokeArguments.add(OperatorNode.create(PhysicalExprOperator.TIMEOUT_REMAINING, TimeUnit.MILLISECONDS));
                        } else if (annotate instanceof Emitter) {
                            if (MetricEmitter.class.isAssignableFrom(parameterType) || TaskMetricEmitter.class.isAssignableFrom(parameterType)) {
                                m.invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "metricEmitter"));
                            } else if (Tracer.class.isAssignableFrom(parameterType)) {
                                m.invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "tracer"));
                            } else {
                                reportMethodParameterException("Trace", method, "@Emitter argument type must be a %s or %s", MetricEmitter.class.getName(), Tracer.class.getName());
                            }
                        }
                    }
                }
            }
            if(inputNext.hasNext()) {
                continue;
            }
            candidates.add(m);
        }
        return candidates;
    }

    class QM {
        Class<? extends Annotation> annotationClass;
        Method method;
        List<OperatorNode<PhysicalExprOperator>> invokeArguments;
        PhysicalExprOperator callOperator;
        final IndexDescriptor.Builder indexBuilder;
        final java.util.Set<String> keyArguments;
        final java.util.Set<String> setArguments;
        boolean singleton;
        boolean batch;
        TypeWidget rowType;
        PropertyAdapter rowProperties;
        String methodType;
        OperatorNode<PhysicalExprOperator> dimensions;
        QM(Class<? extends Annotation> annotationClass, Method method, OperatorNode<PhysicalExprOperator> source, boolean singleton, TypeWidget rowType, OperatorNode<PhysicalExprOperator> dimensions) {
            this.annotationClass = annotationClass;
            this.methodType = annotationClass.getSimpleName();
            this.method = method;
            this.invokeArguments = Lists.newArrayList();
            this.callOperator = PhysicalExprOperator.INVOKEVIRTUAL;
            if (Modifier.isStatic(method.getModifiers())) {
                callOperator = PhysicalExprOperator.INVOKESTATIC;
            } else if(clazz.isInterface()) {
                callOperator = PhysicalExprOperator.INVOKEINTERFACE;
                invokeArguments.add(source);
            } else {
                invokeArguments.add(source);
            }
            indexBuilder = IndexDescriptor.builder();
            keyArguments = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            setArguments = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            this.singleton = singleton;
            this.rowType = rowType;
            this.rowProperties = rowType.getPropertyAdapter();
            this.dimensions = dimensions;
            this.batch = false;
        }

        boolean isScan() {
            return keyArguments.isEmpty();
        }

        OperatorNode<PhysicalExprOperator> invoke() {
            OperatorNode<PhysicalExprOperator> ctx = OperatorNode.create(PhysicalExprOperator.TRACE_CONTEXT, this.dimensions);
            OperatorNode<PhysicalExprOperator> invocation = OperatorNode.create(callOperator, method.getGenericReturnType(), Type.getType(method.getDeclaringClass()), method.getName(), Type.getMethodDescriptor(method), invokeArguments);
            return OperatorNode.create(PhysicalExprOperator.WITH_CONTEXT, ctx, invocation);
        }

        OperatorNode<PhysicalExprOperator> invokeIterable() {
            OperatorNode<PhysicalExprOperator> out = invoke();
            if(singleton) {
                return OperatorNode.create(PhysicalExprOperator.SINGLETON, OperatorNode.create(PhysicalExprOperator.RESOLVE, out));
            } else {
                return out;
            }
        }

        StreamValue stream(CompileContext planner) {
            StreamValue val = StreamValue.iterate(planner, invokeIterable());
            val.add(Location.NONE, StreamOperator.RESOLVE);
            return val;
        }

        void addKeyArgument(Key key, Class<?> parameterType, TypeWidget parameterWidget) {
            String keyName = key.value().toLowerCase();
            boolean skipEmpty = key.skipEmptyOrZero();
            boolean skipNull = key.skipNull();
            if (keyArguments.contains(keyName)) {
                reportMethodParameterException(methodType, method, "@Key('%s') used multiple times", keyName);
            } else if (List.class.isAssignableFrom(parameterType)) {
                if (!batch && !isScan()) {
                    reportMethodParameterException(methodType, method, "@Key column '%s' is a List (batch); a method must either be entirely-batch or entirely-not", keyName);
                }
                batch = true;
                TypeWidget keyType = parameterWidget.getIterableAdapter().getValue();
                verifyArgumentType(methodType, rowType, rowProperties, keyName, keyType, "Key", method);
                addIndexKey(keyName, keyType, skipEmpty, skipNull);
                invokeArguments.add(extractKey(OperatorNode.create(PhysicalExprOperator.LOCAL, "$keys"), keyName));
            } else if (batch) {
                reportMethodParameterException(methodType, method, "@Key column '%s' is a single value but other parameters are batch; a method must either be entirely-batch or entirely-not", keyName);
            } else {
                verifyArgumentType(methodType, rowType, rowProperties, keyName, parameterWidget, "Key", method);
                addIndexKey(keyName, parameterWidget, skipEmpty, skipNull);
                invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"), keyName));
            }
            keyArguments.add(keyName);
        }

        private StreamValue createKeyCursor(CompileContext planner, List<IndexQuery> todo) {
            if (todo.size() == 1) {
                return todo.get(0).keyCursor(planner);
            } else {
                List<StreamValue> cursors = Lists.newArrayListWithExpectedSize(todo.size());
                for (IndexQuery q : todo) {
                    cursors.add(q.keyCursor(planner));
                }
                StreamValue val = StreamValue.merge(planner, cursors);
                val.add(Location.NONE, StreamOperator.DISTINCT);
                return val;
            }
        }

        public void index(List<StreamValue> out, CompileContext planner, List<IndexQuery> todo) {
            StreamValue cursor = createKeyCursor(planner, todo);
            if (batch) {
                // we're a batch API, so we need to get ALL of the queries (and we're not going to handle any followup filters)
                // we only support a single @Key argument and we need a list of keys
                ExprScope scope = new ExprScope();
                scope.addArgument("$keys");
                OperatorNode<PhysicalExprOperator> keys =  cursor.materializeValue();
                StreamValue result = StreamValue.singleton(planner, keys);
                result.add(Location.NONE, StreamOperator.TRANSFORM, scope.createFunction(invoke()));
                result.add(Location.NONE, StreamOperator.RESOLVE);
                if (!singleton) {
                    result.add(Location.NONE, StreamOperator.FLATTEN);
                }
                maybeHandleFilter(todo, result);
                out.add(result);
            } else {
                ExprScope functionScope = new ExprScope();
                functionScope.addArgument("$key");
                OperatorNode<FunctionOperator> function = functionScope.createFunction(invoke());
                cursor.add(Location.NONE, StreamOperator.SCATTER, function);
                cursor.add(Location.NONE, StreamOperator.RESOLVE);
                if (!singleton) {
                    cursor.add(Location.NONE, StreamOperator.FLATTEN);
                }
                OperatorNode<FunctionOperator> isNotNull = OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$"),
                        OperatorNode.create(PhysicalExprOperator.NOT, OperatorNode.create(PhysicalExprOperator.IS_NULL, OperatorNode.create(PhysicalExprOperator.LOCAL, "$$"))));
                cursor.add(source.getLocation(), StreamOperator.FILTER, isNotNull);
                maybeHandleFilter(todo, cursor);
                out.add(cursor);
            }
        }

        private void maybeHandleFilter(List<IndexQuery> todo, StreamValue result) {
            if (todo.size() == 1) {
                IndexQuery q = todo.get(0);
                if (!q.handledFilter) {
                    result.add(q.filterPredicate.getLocation(), StreamOperator.FILTER, q.filterPredicate);
                    q.handledFilter = true;
                }
            }
        }

        void addIndexKey(String keyName, TypeWidget keyType, boolean skipEmpty, boolean skipNull) {
            try {
                indexBuilder.addColumn(keyName, keyType.getValueCoreType(), skipEmpty, skipNull);
            } catch (IllegalArgumentException e) {
                reportMethodParameterException(methodType, method, "Key '%s' cannot be added to index: %s", keyName, e.getMessage());
            }
        }

        public void addSetParameter(String keyName, OperatorNode<PhysicalExprOperator> exprOperatorOperatorNode) {
            if (setArguments.contains(keyName)) {
                reportMethodParameterException(methodType, method, "@Set('%s') used multiple times", keyName);
            }
            setArguments.add(keyName);
            invokeArguments.add(exprOperatorOperatorNode);
        }

        public void verifySetArguments(Map<String, OperatorNode<PhysicalExprOperator>> record) {
            // verify no unknown fields are present in the input
            for(String key : record.keySet()) {
                if(!setArguments.contains(key)) {
                    throw new IllegalArgumentException(String.format("%s::%s Unexpected additional property '%s'", method.getDeclaringClass().getName(), method.getName(), key));
                }
            }
        }

        public int checkSetArguments(Map<String, OperatorNode<PhysicalExprOperator>> record) {
            int out = 0;
            for(String key : record.keySet()) {
                if(setArguments.contains(key)) {
                    out += 1;
                }
            }
            return out;
        }
    }

    private OperatorNode<StreamOperator> accumulate() {
        return OperatorNode.create(StreamOperator.SINK, OperatorNode.create(SinkOperator.ACCUMULATE));
    }


    private OperatorNode<PhysicalExprOperator> extractKey(OperatorNode<PhysicalExprOperator> keys, String keyName) {
        ExprScope scope = new ExprScope();
        OperatorNode<PhysicalExprOperator> key = OperatorNode.create(PhysicalExprOperator.PROPREF, scope.addArgument("$key"), keyName);
        OperatorNode<FunctionOperator> function = scope.createFunction(key);
        return OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, keys, OperatorNode.create(StreamOperator.TRANSFORM, accumulate(), function));
    }

    private OperatorNode<PhysicalExprOperator> getSource(CompileContext planner) {
        if (source == null) {
            if (supplier != null) {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), PhysicalOperator.EVALUATE,
                        OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(PhysicalExprOperator.INVOKEINTERFACE,
                                clazz, Type.getType(Supplier.class), "get", Type.getMethodDescriptor(Type.getType(Object.class)),
                                ImmutableList.of(OperatorNode.create(PhysicalExprOperator.CONSTANT_VALUE, Supplier.class, supplier))));
                source = OperatorNode.create(PhysicalExprOperator.VALUE, value);
            }  else {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), Location.NONE, PhysicalOperator.EVALUATE,
                        OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(PhysicalExprOperator.INVOKENEW,
                                clazz,
                                ImmutableList.of()));
                source = OperatorNode.create(PhysicalExprOperator.VALUE, value);
            }
        }
        return source;
    }

    protected void reportMethodParameterException(String type, Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("@%s method error: %s.%s: %s", type, method.getDeclaringClass().getName(), method.getName(), message));
    }

    private static void reportMethodException(Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("method error: %s.%s: %s", method.getDeclaringClass(), method.getName(), message));
    }

    /**
     * Verifies that the given resultType has a property matching the given fieldName and fieldType.
     */
    private void verifyArgumentType(String methodTypeName, TypeWidget rowType, PropertyAdapter rowProperties, String propertyName, TypeWidget argumentType, String annotationName,
                                    Method method)
            throws ProgramCompileException {
        try {
            TypeWidget targetType = rowProperties.getPropertyType(propertyName);
            if (!targetType.isAssignableFrom(argumentType)) {
                reportMethodParameterException(methodTypeName, method, "class %s property %s is %s while @%s('%s') type is %s: @%s('%s') " +
                                "argument type %s cannot be coerced to property '%s' type %s in method %s.%s",
                        rowType.getTypeName(), propertyName, targetType.getTypeName(), annotationName,
                        propertyName, argumentType.getTypeName(),
                        annotationName, propertyName, argumentType.getTypeName(), propertyName,
                        targetType.getTypeName(), method.getDeclaringClass().getName(),
                        method.getName());
            }
        } catch (PropertyNotFoundException e) {
            reportMethodParameterException(methodTypeName, method, "Property @%s('%s') for method %s.%s does not exist on return type %s",
                    annotationName, propertyName, method.getDeclaringClass().getName(), method.getName(),
                    rowType.getTypeName());
        }
    }

    private Object parseDefaultValue(Method method, String keyName, TypeWidget setType, String defaultValue) {
        if (setType.isIterable()) {
            TypeWidget target = setType.getIterableAdapter().getValue();
            List<Object> expr = Lists.newArrayList();
            StringTokenizer tokenizer = new StringTokenizer(defaultValue, ",", false);
            while (tokenizer.hasMoreElements()) {
                expr.add(parseDefaultValue(method, keyName, target, tokenizer.nextToken().trim()));
            }
            return expr;
        } else {
            try {
                switch (setType.getValueCoreType()) {
                    case BOOLEAN:
                        return Boolean.valueOf(defaultValue);
                    case INT8:
                        return Byte.decode(defaultValue);
                    case INT16:
                        return Short.valueOf(defaultValue);
                    case INT32:
                        return Integer.valueOf(defaultValue);
                    case INT64:
                    case TIMESTAMP:
                        return Long.valueOf(defaultValue);
                    case FLOAT32:
                        return Float.valueOf(defaultValue);
                    case FLOAT64:
                        return Double.valueOf(defaultValue);
                    case STRING:
                        return defaultValue;
                    default:
                        reportMethodException(method, "Unable to match default value for @Set('%s') @DefaultValue('%s') to type %s", keyName, defaultValue, setType.getTypeName());
                        throw new IllegalArgumentException(); // reachability
                }
            } catch (NumberFormatException e) {
                reportMethodException(method, "Unable to parse default argument %s for @Set('%s'): %s", defaultValue, keyName, e.getMessage());
                throw new IllegalArgumentException(); // reachability
            }
        }
    }

    private class RecordWriteVisitor implements Visitor {
        private final Map<String, OperatorNode<PhysicalExprOperator>> record;
        private final CompileContext context;

        RecordWriteVisitor(Map<String, OperatorNode<PhysicalExprOperator>> record, CompileContext context) {
            this.record = record;
            this.context = context;
        }

        @Override
        public boolean visitSet(QM qm, String keyName, Object defaultValue, Class<?> parameterType, TypeWidget setType) {
            if(!record.containsKey(keyName)) {
                if(defaultValue != null) {
                    qm.addSetParameter(keyName, OperatorNode.create(PhysicalExprOperator.CONSTANT, setType, defaultValue));
                } else {
                    return false;
                }
            } else {
                if(defaultValue != null) {
                    qm.addSetParameter(keyName, OperatorNode.create(PhysicalExprOperator.COALESCE, ImmutableList.of(record.get(keyName), OperatorNode.create(PhysicalExprOperator.CONSTANT, setType, defaultValue))));
                }  else {
                    qm.addSetParameter(keyName, OperatorNode.create(PhysicalExprOperator.COALESCE, ImmutableList.of(record.get(keyName), OperatorNode.create(PhysicalExprOperator.THROW,
                            OperatorNode.create(PhysicalExprOperator.INVOKENEW, PropertyNotFoundException.class, ImmutableList.of(context.constant(String.format("Required property '%s' not found on INSERT", keyName))))))));
                }
            }
            return true;
        }
    }

    private class UpdateVisitor extends RecordWriteVisitor {
        UpdateVisitor(Map<String, OperatorNode<PhysicalExprOperator>> record, CompileContext context) {
            super(record, context);
        }

        @Override
        public boolean visitKey(QM qm, Key key, Class<?> parameterClazz, TypeWidget setType) {
            // delete all does not consider any methods with filter arguments
            return false;
        }
    }
}
