/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.runtime;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Ticker;
import com.yahoo.yqlplus.api.trace.*;
import com.yahoo.yqlplus.compiler.runtime.ProgramTracer;
import com.yahoo.yqlplus.engine.scope.MapExecutionScope;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Test
public class ProgramTracerTest {

    public static class DummyTicker extends Ticker {
        private long value;

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public long read() {
            return value;
        }
    }

    public static class MyPayload {

    }

    @Test
    public void testDebugProgramTracer() {
        MapExecutionScope scope = new MapExecutionScope()
                .bind(String.class, "programName", "program")
                .bind(Boolean.class, "debug", true);
        DummyTicker ticker = new DummyTicker();
        ticker.setValue(0L);
        ProgramTracer tracer = new ProgramTracer(ticker, scope);

        Tracer t1 = tracer.start("group", "name01");
        ticker.setValue(MILLISECONDS.toNanos(10L));
        t1.fine("message");
        ticker.setValue(MILLISECONDS.toNanos(15L));
        t1.end();

        Tracer t2 = tracer.start("group2", "name%02d", 2);
        Tracer t3 = t2.start("g3", "name03");
        ticker.setValue(MILLISECONDS.toNanos(16L));
        t3.fine("my fine message");
        ticker.setValue(MILLISECONDS.toNanos(17L));
        t3.end();
        ticker.setValue(MILLISECONDS.toNanos(18L));
        MyPayload myPayload = new MyPayload();
        t2.error(myPayload);
        ticker.setValue(MILLISECONDS.toNanos(20L));
        t2.end();


        TraceRequest req = tracer.createTrace();
        Assert.assertEquals(req.getEntries().size(), 4);
        for (TraceEntry e : req.getEntries()) {
            if ("program".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), 0L);
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(20L));
            } else if ("name01".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), 0L);
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(15L));
                boolean found = false;
                for (TraceLogEntry log : req.getLog()) {
                    if (log.getTraceId() == e.getId()) {
                        found = true;
                        Assert.assertEquals(log.getTicks(), MILLISECONDS.toNanos(10L));
                        Assert.assertEquals(log.get(), "message");
                    }
                }
                Assert.assertEquals(found, true);
            } else if ("name02".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), MILLISECONDS.toNanos(15L));
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(20L));
                boolean found = false;
                for (TraceLogEntry log : req.getLog()) {
                    if (log.getTraceId() == e.getId()) {
                        found = true;
                        Assert.assertEquals(log.getTicks(), MILLISECONDS.toNanos(18L));
                        MyPayload payload = (MyPayload) log.get();
                        Assert.assertEquals(payload, myPayload);
                    }
                }
                Assert.assertEquals(found, true);
            } else if ("name03".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), MILLISECONDS.toNanos(15L));
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(17L));
                boolean found = false;
                for (TraceLogEntry log : req.getLog()) {
                    if (log.getTraceId() == e.getId()) {
                        found = true;
                        Assert.assertEquals(log.getTicks(), MILLISECONDS.toNanos(16L));
                        Assert.assertEquals(log.get(), "my fine message");
                    }
                }
                Assert.assertEquals(found, true);
            } else {
                Assert.fail("Unknown trace entry" + e.getName());
            }
        }
    }

    @Test
    public void testNonDebugProgramTracer() throws IOException {
        MapExecutionScope scope = new MapExecutionScope()
                .bind(String.class, "programName", "program")
                .bind(Boolean.class, "debug", false);
        DummyTicker ticker = new DummyTicker();
        ticker.setValue(0L);
        ProgramTracer tracer = new ProgramTracer(ticker, scope);

        Tracer t1 = tracer.start("group", "name01");
        ticker.setValue(MILLISECONDS.toNanos(10L));
        t1.fine("message");
        ticker.setValue(MILLISECONDS.toNanos(15L));
        t1.end();

        Tracer t2 = tracer.start("group2", "name%02d", 2);
        try {
            Tracer t3 = t2.start("g3", "name03");
            ticker.setValue(MILLISECONDS.toNanos(16L));
            t3.fine("my fine message");
            ticker.setValue(MILLISECONDS.toNanos(17L));
            t3.end();
            throw new Exception("my payload");
        } catch (Exception e) {
            ticker.setValue(MILLISECONDS.toNanos(18L));
            t2.error(e, "exception");
        }
        ticker.setValue(MILLISECONDS.toNanos(20L));
        t2.end();

        TraceRequest req = tracer.createTrace();
        Assert.assertEquals(req.getEntries().size(), 4);
        for (TraceEntry e : req.getEntries()) {
            if ("program".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), 0L);
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(20L));
            } else if ("name01".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), 0L);
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(15L));
                for (TraceLogEntry log : req.getLog()) {
                    if (log.getTraceId() == e.getId()) {
                        Assert.fail("Should not find this FINE log message when debug = false");
                    }
                }
            } else if ("name02".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), MILLISECONDS.toNanos(15L));
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(20L));
                boolean found = false;
                for (TraceLogEntry log : req.getLog()) {
                    if (log.getTraceId() == e.getId()) {
                        found = true;
                        Assert.assertEquals(log.getTicks(), MILLISECONDS.toNanos(18L));
                        ThrowableEvent event = (ThrowableEvent) log.get();
                        Exception payload = (Exception) event.throwable;
                        Assert.assertEquals(payload.getMessage(), "my payload");
                    }
                }
                Assert.assertEquals(found, true);
            } else if ("name03".equals(e.getName())) {
                Assert.assertEquals(e.getStartTicks(), MILLISECONDS.toNanos(15L));
                Assert.assertEquals(e.getEndTicks(), MILLISECONDS.toNanos(17L));
                for (TraceLogEntry log : req.getLog()) {
                    if (log.getTraceId() == e.getId()) {
                        Assert.fail("Should not find this FINE log message when debug = false");
                    }
                }
            } else {
                Assert.fail("Unknown trace entry" + e.getName());
            }
        }

        MappingJsonFactory factory = new MappingJsonFactory();
        StringWriter out = new StringWriter();
        JsonGenerator gen = factory.createGenerator(out);
        gen.writeObject(req);
        gen.flush();
        String val = out.toString();
        String now = ISO8601Utils.format(new Date(req.getStartTime()), true);

        Assert.assertEquals(val, "{\"start\":\"" + now + "\",\"durationUnits\":\"MICROSECONDS\",\"trace\":[{\"group\":\"program\",\"name\":\"program\",\"start\":0,\"end\":20000,\"duration\":20000},{\"group\":\"group\",\"name\":\"name01\",\"start\":0,\"end\":15000,\"duration\":15000},{\"group\":\"group2\",\"name\":\"name02\",\"start\":15000,\"end\":20000,\"duration\":5000,\"log\":[{\"level\":\"SEVERE\",\"t\":18000,\"message\":\"exception\",\"exception\":\"java.lang.Exception\"}],\"trace\":[{\"group\":\"g3\",\"name\":\"name03\",\"start\":15000,\"end\":17000,\"duration\":2000}]}]}");


    }

}
