/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.inject.AbstractModule;
import com.yahoo.yqlplus.engine.ProgramCompiler;
import com.yahoo.yqlplus.engine.compiler.code.ASMClassSourceModule;
import com.yahoo.yqlplus.engine.internal.compiler.PlanProgramCompiler;

public class PlannerCompilerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ProgramCompiler.class).to(PlanProgramCompiler.class);
        install(new ASMClassSourceModule());
    }

}
