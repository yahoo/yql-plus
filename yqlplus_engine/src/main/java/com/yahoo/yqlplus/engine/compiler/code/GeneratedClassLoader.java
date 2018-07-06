/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.Maps;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;

class GeneratedClassLoader extends ClassLoader {
    private final Map<String, byte[]> classBytes = Maps.newHashMap();
    private final Map<String, Class<?>> classes = Maps.newHashMap();

    GeneratedClassLoader(ClassLoader parent) {
        super(parent);
    }

    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, true);
    }

    private Class<?> defineClass(byte[] bytes) {
        Class<?> result = defineClass(null, bytes, 0, bytes.length, getClass().getProtectionDomain());
        classes.put(result.getName(), result);
        return result;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> result = classes.get(name);
        if (result != null) {
            return result;
        }
        byte[] bytes = classBytes.get(name);
        if (bytes != null) {
            return defineClass(bytes);
        }
        result = super.loadClass(name, resolve);
        if (result == null) {
            throw new ClassNotFoundException(name);
        }
        return result;
    }

    public void put(String key, byte[] value) {
        classBytes.put(key, value);
    }

    public Class<?> getClass(String key) {
        return classes.get(key);
    }

    public byte[] getBytes(String name) {
        return classBytes.get(name);
    }

    public void dump(PrintStream out) {
        for (Map.Entry<String, byte[]> e : classBytes.entrySet()) {
            ClassReader reader = new ClassReader(e.getValue());
            TraceClassVisitor visitor = new TraceClassVisitor(new PrintWriter(out, true));
            reader.accept(visitor, 0);
        }
    }

    public void analyze(OutputStream out) {
        for (Map.Entry<String, byte[]> e : classBytes.entrySet()) {
            ClassReader reader = new ClassReader(e.getValue());
            ClassVisitor visitor = new CheckClassAdapter(new TraceClassVisitor(new PrintWriter(out, true)));
            reader.accept(visitor, 0);
        }
    }
}
