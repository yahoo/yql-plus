/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.google.inject.Guice;
import com.yahoo.yqlplus.compiler.generate.GambitSource;
import com.yahoo.yqlplus.compiler.generate.ASMClassSource;
import com.yahoo.yqlplus.compiler.generate.ASMClassSourceModule;
import org.testng.annotations.BeforeMethod;

public class GambitSourceTestBase {
    protected ASMClassSource asm;
    protected GambitSource source;

    @BeforeMethod(alwaysRun = true)
    public void createASMClassSource() {
        asm = Guice.createInjector(new ASMClassSourceModule()).getInstance(ASMClassSource.class);
        source = new GambitSource(asm);
    }
}
