/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */
package com.yahoo.yqlplus.example.apis;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;

public class GeoSourceTest {
    @Test
    public void testGeoSource() throws Exception {
        Injector injector = Guice.createInjector(new SourceModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM geo WHERE text = 'sfo' OUTPUT AS place;");
        Object result = program.run(ImmutableMap.<String, Object>of(), false).getResult("place").get().getResult();
        System.out.println("result " + result);
    }
}
