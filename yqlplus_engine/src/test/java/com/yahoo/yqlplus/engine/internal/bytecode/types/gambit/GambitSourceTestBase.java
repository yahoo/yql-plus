/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.compiler.code.ASMClassSource;
import com.yahoo.yqlplus.engine.compiler.code.GambitSource;
import org.testng.annotations.BeforeMethod;

public class GambitSourceTestBase {
    protected ASMClassSource asm;
    protected GambitSource source;

    @BeforeMethod(alwaysRun = true)
    public void createASMClassSource() {
        asm = new ASMClassSource();
        source = new GambitSource(asm);
    }
}
