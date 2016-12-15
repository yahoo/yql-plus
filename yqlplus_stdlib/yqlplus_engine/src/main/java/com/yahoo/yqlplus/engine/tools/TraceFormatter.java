/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.tools;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.yahoo.yqlplus.api.trace.ThrowableEvent;
import com.yahoo.yqlplus.api.trace.TraceEntry;
import com.yahoo.yqlplus.api.trace.TraceLogEntry;
import com.yahoo.yqlplus.api.trace.TraceRequest;
import com.yahoo.yqlplus.engine.internal.code.CodeFormatter;
import com.yahoo.yqlplus.engine.internal.code.CodeOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Format a trace dump.
 */
public class TraceFormatter {
    public static void dump(OutputStream outputStream, TraceRequest trace) throws IOException {
        Multimap<Integer, TraceEntry> childmap = ArrayListMultimap.create();
        for (TraceEntry entry : trace.getEntries()) {
            childmap.put(entry.getParentId(), entry);
        }
        Multimap<Integer, TraceLogEntry> logmap = ArrayListMultimap.create();
        for (TraceLogEntry entry : trace.getLog()) {
            logmap.put(entry.getTraceId(), entry);
        }

        CodeOutput out = new CodeOutput();
        dumpTree(out, childmap, logmap, childmap.get(0));
        outputStream.write(out.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void dumpTree(CodeOutput out, Multimap<Integer, TraceEntry> childmap, Multimap<Integer, TraceLogEntry> logmap, Collection<TraceEntry> traceEntries) {
        for (TraceEntry entry : traceEntries) {
            out.println(String.format("%s : %s (%.2f - %.2f) (%.2f)", entry.getGroup(), entry.getName(), entry.getStartMilliseconds(), entry.getEndMilliseconds(), entry.getDurationMilliseconds()));
            out.indent();
            for (TraceLogEntry log : logmap.get(entry.getId())) {
                out.println("%6d [%s]: %s", TimeUnit.NANOSECONDS.toMicros(log.getTicks()), log.getLevel(), log);
                if (log.get() instanceof ThrowableEvent) {
                    ThrowableEvent event = (ThrowableEvent) log.get();
                    out.indent();
                    out.println("Exception: %s", event.message);
                    out.indent();
                    CodeFormatter.writeException(event.throwable, out);
                    out.dedent();
                    out.dedent();
                }
            }
            dumpTree(out, childmap, logmap, childmap.get(entry.getId()));
            out.dedent();
        }
    }
}
