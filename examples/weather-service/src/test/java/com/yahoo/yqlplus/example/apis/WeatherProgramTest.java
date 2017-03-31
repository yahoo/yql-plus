/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.example.apis;

import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;

public class WeatherProgramTest {
    @Test
    public void testWeatherSource() throws Exception {
        Injector injector = Guice.createInjector(new SourceModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        String programStr = IOUtils.toString(this.getClass().getResourceAsStream("/programs/weather.program"), "UTF-8");
        CompiledProgram program = compiler.compile(programStr);
        Object result = program.run(ImmutableMap.<String, Object>of("text", "sfo"), false).getResult("weather").get().getResult();
        System.out.println("result " + result);
    }
}
