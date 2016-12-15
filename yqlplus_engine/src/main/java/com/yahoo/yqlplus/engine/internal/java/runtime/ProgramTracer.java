/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.runtime;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Ticker;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.yahoo.yqlplus.api.annotations.ExecuteScoped;
import com.yahoo.yqlplus.api.trace.*;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

@ExecuteScoped
public class ProgramTracer implements RequestTracer {
    private final long started;
    private final boolean debug;
    private final RelativeTicker ticker;
    private final Tracer programTracer;
    private final ConcurrentLinkedQueue<Entry> entries;
    private final ConcurrentLinkedQueue<TraceLogEntry> log;
    private final AtomicInteger idSource = new AtomicInteger(0);

    final class Entry implements TraceEntry {
        final long start;
        long stop;
        final int parent;
        final String group;
        final String name;
        final int id;

        Entry(int parent, long start, String group, String name) {
            this.parent = parent;
            this.start = start;
            this.group = group;
            this.name = name;
            this.id = idSource.incrementAndGet();
        }

        @Override
        public TimeUnit getTickUnits() {
            return TimeUnit.NANOSECONDS;
        }

        @Override
        public long getStartTicks() {
            return start;
        }

        @Override
        public long getEndTicks() {
            return stop;
        }

        @Override
        public long getDurationTicks() {
            return stop - start;
        }

        @Override
        public float getStartMilliseconds() {
            float ratio = getTickUnits().convert(1L, TimeUnit.MILLISECONDS);
            return (float) getStartTicks() / ratio;
        }

        @Override
        public float getEndMilliseconds() {
            float ratio = getTickUnits().convert(1L, TimeUnit.MILLISECONDS);
            return (float) getEndTicks() / ratio;
        }

        @Override
        public float getDurationMilliseconds() {
            float ratio = getTickUnits().convert(1L, TimeUnit.MILLISECONDS);
            return (float) getDurationTicks() / ratio;
        }

        @Override
        public String getGroup() {
            return group;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getParentId() {
            return parent;
        }

        @Override
        public int getId() {
            return id;
        }
    }

    static final class Request implements TraceRequest, JsonSerializable {
        private final long startTime;
        private final List<? extends TraceEntry> entries;
        private final List<? extends TraceLogEntry> log;

        Request(long startTime, List<? extends TraceEntry> entries, List<? extends TraceLogEntry> log) {
            this.startTime = startTime;
            this.entries = entries;
            this.log = log;
        }

        @Override
        public long getStartTime() {
            return startTime;
        }

        @Override
        public List<? extends TraceEntry> getEntries() {
            return entries;
        }

        @Override
        public List<? extends TraceLogEntry> getLog() {
            return log;
        }

        @Override
        public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            // hm, do we want a flat JSON representation or a structured one?
            // let's go with structured for now

            // in which case we're going to build an index of entries and log entries
            jgen.writeStartObject();
            jgen.writeStringField("start", ISO8601Utils.format(new Date(startTime), true));
            jgen.writeStringField("durationUnits", "MICROSECONDS");

            Multimap<Integer, TraceEntry> childmap = ArrayListMultimap.create();
            Multimap<Integer, TraceLogEntry> logmap = ArrayListMultimap.create();
            for (TraceEntry entry : getEntries()) {
                childmap.put(entry.getParentId(), entry);
            }
            for (TraceLogEntry entry : getLog()) {
                logmap.put(entry.getTraceId(), entry);
            }

            writeTraceEntries(jgen, provider, childmap, logmap, childmap.get(0));

            jgen.writeEndObject();

        }

        private void writeTraceEntries(JsonGenerator jgen, SerializerProvider provider, Multimap<Integer, TraceEntry> childmap, Multimap<Integer, TraceLogEntry> logmap, Collection<TraceEntry> traceEntries) throws IOException {
            if (traceEntries.isEmpty()) {
                return;
            }
            jgen.writeArrayFieldStart("trace");
            for (TraceEntry entry : traceEntries) {
                jgen.writeStartObject();
                jgen.writeStringField("group", entry.getGroup());
                jgen.writeStringField("name", entry.getName());
                jgen.writeNumberField("start", TimeUnit.MICROSECONDS.convert(entry.getStartTicks(), entry.getTickUnits()));
                jgen.writeNumberField("end", TimeUnit.MICROSECONDS.convert(entry.getEndTicks(), entry.getTickUnits()));
                jgen.writeNumberField("duration", TimeUnit.MICROSECONDS.convert(entry.getDurationTicks(), entry.getTickUnits()));
                Collection<TraceLogEntry> log = logmap.get(entry.getId());
                if (!log.isEmpty()) {
                    jgen.writeArrayFieldStart("log");
                    for (TraceLogEntry logEntry : log) {
                        provider.defaultSerializeValue(logEntry, jgen);
                    }
                    jgen.writeEndArray();
                }
                writeTraceEntries(jgen, provider, childmap, logmap, childmap.get(entry.getId()));
                jgen.writeEndObject();
            }
            jgen.writeEndArray();

        }

        @Override
        public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException, JsonProcessingException {
            serialize(jgen, provider);
        }
    }

    @Inject
    ProgramTracer(ExecutionScope scope) {
        this(Ticker.systemTicker(), scope);
    }

    ProgramTracer(Ticker ticker, ExecutionScope scope) {
        this(ticker, scope.get(Key.get(Boolean.class, Names.named("debug"))), "program", scope.get(Key.get(String.class, Names.named("programName"))));
    }

    public ProgramTracer(Ticker ticker, boolean debug, String rootGroup, String rootName) {
        this.started = System.currentTimeMillis();
        this.ticker = new RelativeTicker(ticker);
        this.debug = debug;
        this.entries = new ConcurrentLinkedQueue<>();
        this.log = new ConcurrentLinkedQueue<>();
        this.programTracer = start(rootGroup, rootName);
    }

    @Override
    public long elapsedTicks() {
        return ticker.read();
    }

    @Override
    public Timeout createTimeout(final long timeout, final TimeUnit timeoutUnits) {
        return new TimeoutTracker(timeout, timeoutUnits, ticker);
    }

    @Override
    public TimeUnit getTickUnits() {
        return TimeUnit.NANOSECONDS;
    }

    private String maybeFormat(String message, Object... args) {
        if (args != null) {
            return String.format(message, args);
        }
        return message;
    }

    @Override
    public Tracer start(final String group, String name, Object... args) {
        return start(group, maybeFormat(name, args));
    }

    @Override
    public Tracer start(String group, String name) {
        final Entry entry = new Entry(0, elapsedTicks(), group, name);
        entries.add(entry);
        return new EntryTracer(entry);
    }

    @Override
    public String getGroup() {
        return programTracer.getGroup();
    }

    @Override
    public String getName() {
        return programTracer.getName();
    }


    @Override
    public void fine(String message) {
        programTracer.fine(message);
    }

    @Override
    public void fine(String message, Object arg0, Object... args) {
        programTracer.fine(message, arg0, args);
    }

    @Override
    public void fine(String message, Object arg0) {
        programTracer.fine(message, arg0);
    }

    @Override
    public void fine(Object message) {
        programTracer.fine(message);
    }

    @Override
    public void error(String message) {
        programTracer.error(message);
    }

    @Override
    public void error(String message, Object arg0) {
        programTracer.error(message, arg0);
    }

    @Override
    public void error(String message, Object arg0, Object... args) {
        programTracer.error(message, arg0, args);
    }

    @Override
    public void error(Throwable t, String message) {
        programTracer.error(t, message);
    }

    @Override
    public void error(Throwable t, String message, Object arg0) {
        programTracer.error(t, message, arg0);
    }

    @Override
    public void error(Throwable t, String message, Object arg0, Object... args) {
        programTracer.error(t, message, arg0, args);
    }

    @Override
    public void error(Object message) {
        programTracer.error(message);
    }

    @Override
    public void end() {
        programTracer.end();
    }

    @Override
    public void close() {
        end();
    }

    @Override
    public TraceRequest createTrace() {
        end();
        return new Request(started, ImmutableList.copyOf(entries), ImmutableList.copyOf(log));
    }

    private class EntryTracer extends LoggingTracer implements Tracer {
        private final Entry entry;

        public EntryTracer(Entry entry) {
            super(entry.id, ticker, log, debug ? Level.FINE : Level.SEVERE);
            this.entry = entry;
        }

        @Override
        public Tracer start(String group, String name, Object... args) {
            final String formattedName = maybeFormat(name, args);
            return start(group, formattedName);
        }

        @Override
        public Tracer start(String group, String name) {
            final Entry entry = new Entry(this.entry.id, elapsedTicks(), group, name);
            entries.add(entry);
            return new EntryTracer(entry);
        }

        @Override
        public String getGroup() {
            return entry.group;
        }

        @Override
        public String getName() {
            return entry.name;
        }


        @Override
        public synchronized void end() {
            if (entry.stop == 0L) {
                entry.stop = elapsedTicks();
            }
        }

        @Override
        public void close() {
            end();
        }
    }
}
