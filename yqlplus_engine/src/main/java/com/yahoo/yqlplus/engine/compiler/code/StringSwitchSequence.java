/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.Maps;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.Map;

public class StringSwitchSequence implements BytecodeSequence {
    private Map<String, BytecodeSequence> sequenceMap = Maps.newHashMap();
    private BytecodeSequence defaultSequence = BytecodeSequence.NOOP;
    private BytecodeExpression input;
    private final boolean caseInsensitive;

    public StringSwitchSequence(BytecodeExpression input, boolean caseInsensitive) {
        this.input = input;
        this.caseInsensitive = caseInsensitive;
    }

    public void put(String key, BytecodeSequence value) {
        sequenceMap.put(key, value);
    }

    public void setDefaultSequence(BytecodeSequence defaultSequence) {
        this.defaultSequence = defaultSequence;
    }

    @Override
    public void generate(CodeEmitter code) {
        Map<String, Label> labelMap = Maps.newHashMapWithExpectedSize(sequenceMap.size());
        for(String key : sequenceMap.keySet()) {
            labelMap.put(key, new Label());
        }
        Label done = new Label();
        Label defaultCase =  defaultSequence == BytecodeSequence.NOOP ?  done : new Label();
        code.exec(input);
        code.emitInstanceCheck(input.getType(), String.class, defaultCase);
        code.emitStringSwitch(labelMap, defaultCase, caseInsensitive);
        MethodVisitor mv = code.getMethodVisitor();
        for(Map.Entry<String, BytecodeSequence>  e : sequenceMap.entrySet()) {
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
