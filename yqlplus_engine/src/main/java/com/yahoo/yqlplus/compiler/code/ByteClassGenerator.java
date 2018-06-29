/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.util.Iterator;
import java.util.Map;

class ByteClassGenerator implements ClassSink, Iterable<Map.Entry<String, byte[]>> {
    private Map<String, ClassWriter> writers = Maps.newLinkedHashMap();

    @Override
    public ClassVisitor create(String internalName) {
        ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES + ClassWriter.COMPUTE_MAXS) {
            @Override
            protected String getCommonSuperClass(String type1, String type2) {
                // crude, incorrect hack -- we should review the list of to-be-generated-classes and
                // see if any match. for now we'll just handle class-not-found with 'must be Object'
                // the superclass tries to *load* the two types to identify the common supertype
                // (using the ClassWriter's own classloader)
                try {
                    return super.getCommonSuperClass(type1, type2);
                } catch (RuntimeException e) {
                    // but we can't do that because one or both of these classes do not exist
                    return "java/lang/Object";
                }
            }
        };
        writers.put(internalName, classWriter);
        return classWriter;
    }

    @Override
    public Iterator<Map.Entry<String, byte[]>> iterator() {
        return Iterators.transform(writers.entrySet().iterator(), new Function<Map.Entry<String, ClassWriter>, Map.Entry<String, byte[]>>() {
            @Override
            public Map.Entry<String, byte[]> apply(final Map.Entry<String, ClassWriter> input) {
                final byte[] data = input.getValue().toByteArray();
                return new Map.Entry<String, byte[]>() {
                    @Override
                    public String getKey() {
                        return input.getKey();
                    }

                    @Override
                    public byte[] getValue() {
                        return data;
                    }

                    @Override
                    public byte[] setValue(byte[] value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        });
    }
}
