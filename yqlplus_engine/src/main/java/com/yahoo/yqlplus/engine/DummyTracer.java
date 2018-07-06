/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.yahoo.yqlplus.api.trace.Tracer;

public class DummyTracer implements Tracer {
    public static final Tracer instance = new DummyTracer();

    @Override
    public String getGroup() {
        return "";
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    public Tracer start(String group, String name, Object... args) {
        return this;
    }

    @Override
    public Tracer start(String group, String name) {
        return this;
    }

    @Override
    public void error(String message) {

    }

    @Override
    public void error(String message, Object arg0) {

    }

    @Override
    public void error(String message, Object arg0, Object... args) {

    }

    @Override
    public void error(Throwable t, String message) {

    }

    @Override
    public void error(Throwable t, String message, Object arg0) {

    }

    @Override
    public void error(Throwable t, String message, Object arg0, Object... args) {

    }

    @Override
    public void error(Object message) {

    }

    @Override
    public void fine(String message) {

    }

    @Override
    public void fine(String message, Object arg0, Object... args) {

    }

    @Override
    public void fine(String message, Object arg0) {

    }

    @Override
    public void fine(Object message) {

    }

    @Override
    public void end() {

    }

    @Override
    public void close() {

    }
}
