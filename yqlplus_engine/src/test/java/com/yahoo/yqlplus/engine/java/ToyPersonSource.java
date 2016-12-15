/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class ToyPersonSource extends ToyMemoryTableSource<Person> {
    private final Map<String, Person> idMap = Maps.newHashMap();
    private final Map<Integer, Person> iidMap = Maps.newHashMap();
    private final Multimap<String, Person> nameMap = ArrayListMultimap.create();
    private final List<Person> persons = Lists.newArrayList();
    private final ScheduledExecutorService toyWorker;

    @Inject
    ToyPersonSource(@Named("toy") ScheduledExecutorService toyWorker) {
        this.toyWorker = toyWorker;
        try {
            load(ToyPersonSource.class.getResourceAsStream("persons.json"), Person.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void ingest(Person record) {
        idMap.put(record.getId(), record);
        nameMap.put(record.getValue(), record);
        iidMap.put(record.getIid(), record);
        persons.add(record);
    }

    @Query
    public List<Person> scan() {
        return persons;
    }

    @Query
    public Person lookup(@Key("id") String id) {
        return idMap.get(id);
    }

    @Query
    public Iterable<Person> lookupBatch(@Key("otherId") List<String> id) {
        return Iterables.transform(id, new Function<String, Person>() {
            @Nullable
            @Override
            public Person apply(@Nullable String input) {
                return idMap.get(input);
            }
        });
    }

    @Query
    public Iterable<Person> lookup(@Key("iid") List<Integer> iids) {
        return Iterables.transform(iids, new Function<Integer, Person>() {
            @Nullable
            @Override
            public Person apply(@Nullable Integer input) {
                return iidMap.get(input);
            }
        });
    }

    @Query
    public Iterable<Person> lookupName(@Key("value") String value) {
        return nameMap.get(value);
    }

    private <V> ListenableFuture<V> eventually(long delay, final Callable<V> todo) {
        final SettableFuture<V> result = SettableFuture.create();
        toyWorker.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    result.set(todo.call());
                } catch (Exception e) {
                    result.setException(e);
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
        return result;
    }

    @Query
    public ListenableFuture<List<Person>> futureScan(int delay) {
        return eventually(delay, new Callable<List<Person>>() {
            @Override
            public List<Person> call() throws Exception {
                return scan();
            }
        });
    }

    @Query
    public ListenableFuture<Person> lookup(@Key("id") final String id, int delay) {
        return eventually(delay, new Callable<Person>() {
            @Override
            public Person call() throws Exception {
                return lookup(id);
            }
        });
    }

    @Query
    public ListenableFuture<Iterable<Person>> lookupBatch(@Key("otherId") final List<String> id, int delay) {
        return eventually(delay, new Callable<Iterable<Person>>() {
            @Override
            public Iterable<Person> call() throws Exception {
                return lookupBatch(id);
            }
        });
    }

    @Query
    public ListenableFuture<Iterable<Person>> lookup(@Key("iid") final List<Integer> iids, int delay) {
        return eventually(delay, new Callable<Iterable<Person>>() {
            @Override
            public Iterable<Person> call() throws Exception {
                return lookup(iids);
            }
        });
    }

    @Query
    public ListenableFuture<Iterable<Person>> lookupName(@Key("value") final String value, int delay) {
        return eventually(delay, new Callable<Iterable<Person>>() {
            @Override
            public Iterable<Person> call() throws Exception {
                return lookupName(value);
            }
        });
    }
}
