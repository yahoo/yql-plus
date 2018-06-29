/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.collect.Lists;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

class TraceClassGenerator implements ClassSink {
    private List<StringWriter> writers = Lists.newArrayList();

    @Override
    public ClassVisitor create(String internalName) {
        StringWriter output = new StringWriter();
        TraceClassVisitor tcw = new TraceClassVisitor(new PrintWriter(output));
        writers.add(output);
        return tcw;
    }

    public List<StringWriter> getWriters() {
        return writers;
    }
}
