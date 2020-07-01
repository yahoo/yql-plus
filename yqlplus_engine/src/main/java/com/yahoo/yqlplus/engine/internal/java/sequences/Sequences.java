/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.sequences;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.yahoo.yqlplus.api.trace.Timeout;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.java.runtime.TimeoutHandler;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public final class Sequences {
    private Sequences() {

    }

    public static final int NO_LIMIT = -1;
    public static final int NO_OFFSET = 0;

    public static final class EmptyRecord implements Record, JsonSerializable {
        private EmptyRecord() {
        }

        @Override
        public Iterable<String> getFieldNames() {
            return ImmutableList.of();
        }

        @Override
        public Object get(String field) {
            throw new IllegalArgumentException("Field '" + field + "' not found");
        }

        @Override
        public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeEndObject();
        }

        @Override
        public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeEndObject();
        }
    }

    private static final Record EMPTY_RECORD = new EmptyRecord();

    public static List<Record> singletonSequence() {
        return ImmutableList.of(EMPTY_RECORD);
    }

    public static <ROW> Iterable<ROW> doSlice(Iterable<ROW> source, int limit, int offset) {
        Preconditions.checkArgument(offset >= 0, "offset >= 0");
        if (offset > 0) {
            source = Iterables.skip(source, offset);
        }
        if (limit >= 0) {
            source = Iterables.limit(source, limit);
        }
        return source;
    }

    public static <ROW> List<ROW> doSort(Iterable<ROW> input, Comparator<ROW> comparator) {
        ArrayList<ROW> rows = Lists.newArrayList(input);
        Collections.sort(rows, comparator);
        return rows;
    }

    public static <ROW> List<ROW> doMerge(List<ROW>... inputs) {
        int count = 0;
        for (List<ROW> input : inputs) {
            count += input.size();
        }
        List<ROW> result = Lists.newArrayListWithExpectedSize(count);
        for (List<ROW> input : inputs) {
            result.addAll(input);
        }
        return result;
    }

    public static <ROW, LEFT, RIGHT, KEY> List<ROW> doJoin(boolean inner, List<LEFT> left, List<RIGHT> right, List<KEY> leftKeys, Function<RIGHT, KEY> rightKey, JoinEmit<ROW, LEFT, RIGHT> output) {
        List<ROW> result = Lists.newArrayList();
        Multimap<KEY, RIGHT> rights = ArrayListMultimap.create(leftKeys.size(), 4);
        for (RIGHT row : right) {
            rights.put(rightKey.apply(row), row);
        }
        for (int i = 0; i < left.size(); ++i) {
            LEFT leftRow = left.get(i);
            KEY leftKey = leftKeys.get(i);
            Collection<RIGHT> match = rights.get(leftKey);
            if (match == null || match.isEmpty()) {
                if (!inner) {
                    ROW row = output.create();
                    output.setLeft(row, leftRow);
                    result.add(row);
                }
                continue;
            }
            for (RIGHT rightRow : match) {
                ROW row = output.create();
                output.setLeft(row, leftRow);
                output.setRight(row, rightRow);
                result.add(row);
            }
        }
        return result;
    }

    public static <ROW> List<ROW> singletonTransform(ROW row) {
        if (row == null) {
            return ImmutableList.of();
        } else {
            return ImmutableList.of(row);
        }
    }

    public static <ROW> ListenableFuture<List<ROW>> singletonTransformFuture(ListenableFuture<ROW> row) {
        return Futures.transform(row, new Function<ROW, List<ROW>>() {
            @Nullable
            @Override
            public List<ROW> apply(@Nullable ROW input) {
                return singletonTransform(input);
            }
        });
    }

    public static <KEY> List<KEY> assembleKeys(List<KEY> singleKeys, List<List<KEY>> listKeys) {
        List<KEY> result = Lists.newArrayList();
        Set<KEY> seen = Sets.newHashSet();
        for (KEY key : singleKeys) {
            if (seen.add(key)) {
                result.add(key);
            }
        }
        for (KEY key : Iterables.concat(listKeys)) {
            if (seen.add(key)) {
                result.add(key);
            }
        }
        return result;
    }

    public static <SET> List<SET> assembleSets(List<SET> sets) {
        List<SET> result = Lists.newArrayList();
        for (SET set : sets) {
            result.add(set);
        }
        return result;
    }

    private static <SEQUENCE, KEY> Callable<SEQUENCE> createJob(final Tracer tracer, final int idx, final Function<KEY, SEQUENCE> source, final KEY key) {
        return new Callable<SEQUENCE>() {
            @Override
            public SEQUENCE call() throws Exception {
                Tracer childTracer = tracer.start(tracer.getGroup(), tracer.getName() + "." + idx);
                try {
                    return source.apply(key);
                } finally {
                    childTracer.end();
                }
            }
        };
    }

    private static <SEQUENCE, SET> Callable<SEQUENCE> createJob(final Tracer tracer, final Function<List<SET>, SEQUENCE> source,
                                                                final List<SET> keys) {
        return new Callable<SEQUENCE>() {
            @Override
            public SEQUENCE call() throws Exception {
                Tracer childTracer = tracer.start(tracer.getGroup(), tracer.getName());
                try {
                    return source.apply(keys);
                } finally {
                    childTracer.end();
                }
            }
        };
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, KEY> ListenableFuture<List<ROW>> invokeAsyncScatter(final Executor executor, final AsyncFunction<KEY, SEQUENCE> source, List<KEY> keys, Tracer tracer, Timeout timeout, TimeoutHandler handler) throws Exception {
        List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        int idx = -1;
        for (KEY key : keys) {
            if (key != null) {
                final Tracer childTracer = tracer.start(tracer.getGroup(), tracer.getName() + "." + (++idx));
                ListenableFuture<SEQUENCE> result = source.apply(key);
                results.add(result);
                result.addListener(new Runnable() {
                    @Override
                    public void run() {
                        childTracer.end();
                    }
                }, MoreExecutors.newDirectExecutorService());
            }
        }
        ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        final int estimatedResultSize = results.size();
        return handler.withTimeout(gatherResults(executor, gather, estimatedResultSize), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, KEY> ListenableFuture<List<ROW>> invokeScatter(final ListeningExecutorService workExecutor, final Function<KEY, SEQUENCE> source, final List<KEY> keys, Tracer tracer, Timeout timeout, TimeoutHandler handler) throws Exception {
        List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        int idx = -1;
        for (KEY key : keys) {
            if (key != null) {
                results.add(workExecutor.submit(createJob(tracer, ++idx, source, key)));
            }
        }
        ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        final int estimatedResultSize = results.size();
        return handler.withTimeout(gatherResults(workExecutor, gather, estimatedResultSize), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, SET> ListenableFuture<List<ROW>> invokeAsyncSet(
            final Executor executor, final AsyncFunction<List<SET>, SEQUENCE> source, List<SET> keys, Tracer tracer,
            Timeout timeout, TimeoutHandler handler) throws Exception {
        // TODO OPTIMIZE: List not needed in this case
        List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        final Tracer childTracer = tracer.start(tracer.getGroup(), tracer.getName());
        ListenableFuture<SEQUENCE> result = source.apply(keys);
        results.add(result);
        result.addListener(new Runnable() {
            @Override
            public void run() {
                childTracer.end();
            }
        }, MoreExecutors.newDirectExecutorService());
        ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        return handler.withTimeout(gatherResults(executor, gather, 1), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, SET> ListenableFuture<List<ROW>> invokeSet(
            final ListeningExecutorService workExecutor, final Function<List<SET>, SEQUENCE> source, final List<SET> keys,
            Tracer tracer, Timeout timeout, TimeoutHandler handler) throws Exception {
        // TODO OPTIMIZE: List not needed in this case
        List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        results.add(workExecutor.submit(createJob(tracer, source, keys)));
        ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        return handler.withTimeout(gatherResults(workExecutor, gather, 1), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, SET> ListenableFuture<List<ROW>> invokeAsyncBatchSet(
            final Executor executor, final AsyncFunction<List<SET>, SEQUENCE> source, List<SET> keys, Tracer tracer,
            Timeout timeout, TimeoutHandler handler) throws Exception {
        List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        final Tracer childTracer = tracer.start(tracer.getGroup(), tracer.getName());
        List<SET> methodArgs = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) != null) {
                methodArgs.add(keys.get(i));
            } else {
                ListenableFuture<SEQUENCE> result = source.apply(methodArgs);
                results.add(result);
                result.addListener(new Runnable() {
                    @Override
                    public void run() {
                        childTracer.end();
                    }
                }, MoreExecutors.newDirectExecutorService());
                methodArgs = Lists.newArrayList();
            }
        }
        ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        return handler.withTimeout(gatherResults(executor, gather, 1), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, SET> ListenableFuture<List<ROW>> invokeBatchSet(
            final ListeningExecutorService workExecutor, final Function<List<SET>, SEQUENCE> source, final List<SET> keys,
            Tracer tracer, Timeout timeout, TimeoutHandler handler) throws Exception {
        List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        List<SET> methodArgs = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) != null) {
                methodArgs.add(keys.get(i));
            } else {
                results.add(workExecutor.submit(createJob(tracer, source, methodArgs)));
                methodArgs = Lists.newArrayList();
            }
        }
        ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        return handler.withTimeout(gatherResults(workExecutor, gather, 1), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, SET> ListenableFuture<List<ROW>> invokeAsyncBatchSet(
            final Executor executor, final AsyncFunction<List<SET>, SEQUENCE> source, final Tracer tracer,
            final Timeout timeout, final TimeoutHandler handler, final List<ROW>... inputs) throws Exception {
        final List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        final Tracer childTracer = tracer.start(tracer.getGroup(), tracer.getName());
        for (List<ROW> input : inputs) {
            for (int i = 0; i < input.size(); i++) {
                Record record = (Record) input.get(i);
                List methodArgs = Lists.newArrayList();
                Iterable<String> fieldNames = record.getFieldNames();
                for (String fieldName : fieldNames) {
                    methodArgs.add(record.get(fieldName));
                }
                ListenableFuture<SEQUENCE> result = source.apply(methodArgs);
                results.add(result);
                result.addListener(new Runnable() {
                    @Override
                    public void run() {
                        childTracer.end();
                    }
                }, MoreExecutors.newDirectExecutorService());
            }
        }
        final ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        return handler.withTimeout(gatherResults(executor, gather, 1), timeout.verify(), timeout.getTickUnits());
    }

    public static <ROW, SEQUENCE extends Iterable<ROW>, SET> ListenableFuture<List<ROW>> invokeBatchSet(
            final ListeningExecutorService workExecutor, final Function<List<SET>, SEQUENCE> source,
            final Tracer tracer, final Timeout timeout, final TimeoutHandler handler, final List<ROW>... inputs) throws Exception {
        final List<ListenableFuture<SEQUENCE>> results = Lists.newArrayList();
        for (List<ROW> input : inputs) {
            for (int i = 0; i < input.size(); i++) {
                Record record = (Record) input.get(i);
                List methodArgs = Lists.newArrayList();
                Iterable<String> fieldNames = record.getFieldNames();
                for (String fieldName : fieldNames) {
                    methodArgs.add(record.get(fieldName));
                }
                results.add(workExecutor.submit(createJob(tracer, source, methodArgs)));
            }
        }
        final ListenableFuture<List<SEQUENCE>> gather = Futures.allAsList(results);
        return handler.withTimeout(gatherResults(workExecutor, gather, 1), timeout.verify(), timeout.getTickUnits());
    }

    private static <ROW, SEQUENCE extends Iterable<ROW>> ListenableFuture<List<ROW>> gatherResults(Executor executor, ListenableFuture<List<SEQUENCE>> gather, final int estimatedResultSize) {
        final SettableFuture<List<ROW>> target = SettableFuture.create();
        Futures.addCallback(gather, new FutureCallback<List<SEQUENCE>>() {
            @Override
            public void onSuccess(List<SEQUENCE> result) {
                List<ROW> data = Lists.newArrayListWithExpectedSize(estimatedResultSize);
                for (SEQUENCE item : result) {
                    if (item != null) {
                        for (ROW row : item) {
                            data.add(row);
                        }
                    }
                }
                target.set(data);
            }

            @Override
            public void onFailure(Throwable t) {
                target.setException(t);
            }
        }, executor);
        return target;
    }

    public static Map<String, Object> createMap() {
        return ImmutableMap.of();
    }

    public static Map<String, Object> createMap(Object... datums) {
        ImmutableMap.Builder<String, Object> output = ImmutableMap.builder();
        if (datums != null) {
            for (int i = 0; i < datums.length; i += 2) {
                if (datums[i + 1] != null) {
                    output.put((String) datums[i], datums[i + 1]);
                }
            }
        }
        return output.build();
    }

}
