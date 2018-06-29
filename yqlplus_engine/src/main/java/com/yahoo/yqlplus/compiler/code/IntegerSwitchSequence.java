/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.collect.Maps;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.Map;

public class IntegerSwitchSequence implements BytecodeSequence {
    private Map<Integer, BytecodeSequence> sequenceMap = Maps.newHashMap();
    private BytecodeSequence defaultSequence = BytecodeSequence.NOOP;

    public void put(Integer key, BytecodeSequence value) {
        sequenceMap.put(key, value);
    }

    public void setDefaultSequence(BytecodeSequence defaultSequence) {
        this.defaultSequence = defaultSequence;
    }

    @Override
    public void generate(CodeEmitter code) {
        Map<Integer, Label> labelMap = Maps.newHashMapWithExpectedSize(sequenceMap.size());
        for(Integer key : sequenceMap.keySet()) {
            labelMap.put(key, new Label());
        }
        Label done = new Label();
        Label defaultCase =  defaultSequence == BytecodeSequence.NOOP ?  done : new Label();
        code.emitIntegerSwitch(labelMap, defaultCase);
        MethodVisitor mv = code.getMethodVisitor();
        for(Map.Entry<Integer,BytecodeSequence>  e : sequenceMap.entrySet()) {
            mv.visitLabel(labelMap.get(e.getKey()));
            code.exec(e.getValue());
            mv.visitJumpInsn(Opcodes.GOTO, done);
        }
        if(defaultCase != done) {
            mv.visitLabel(defaultCase);
            code.exec(defaultSequence);
        }
        mv.visitLabel(done);

    }
}
