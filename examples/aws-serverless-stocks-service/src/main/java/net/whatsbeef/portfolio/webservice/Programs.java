/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package net.whatsbeef.portfolio.webservice;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import org.antlr.v4.runtime.RecognitionException;
import org.apache.commons.io.IOUtils;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

public class Programs {

    // map of path and program
    private static Map<String, CompiledProgram> programMap;
    private static final Gson GSON = new Gson();
    static {
        initProgramMap();
    }

    private static void initProgramMap() {
        programMap = new HashMap<>();
        try {
            compilePrograms();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void compilePrograms() throws IOException {
        String stockProgram = IOUtils.toString(Programs.class.getResourceAsStream("/programs/getStocks.yql"), "UTF-8");
        String stockPutProgram = IOUtils.toString(Programs.class.getResourceAsStream("/programs/putStock.yql"), "UTF-8");
        String stockUpdateProgram = IOUtils.toString(Programs.class.getResourceAsStream("/programs/updateStocks.yql"), "UTF-8");
        Injector injector = Guice.createInjector(new SourceModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        try {
            programMap.put("getStockPrices", compiler.compile(stockProgram));
            programMap.put("putStock", compiler.compile(stockPutProgram));
            programMap.put("updateStockPrices", compiler.compile(stockUpdateProgram));
        } catch (RecognitionException | IOException e) {
            throw new ProgramCompileException("Compiling program: " + stockProgram + " failed ", e);
        }
    }

    public static CompiledProgram getProgram(String path) {
        return programMap.get(path);
    }

    public static String runProgram(String path, Map<String, Object> params, String programResultName) throws IOException  {
        try {
            Object results = programMap.get(path).run(params, true).getResult(programResultName).get().getResult();
            return GSON.toJson(results);
        } catch (Exception e) {
            throw new IOException("Executing program failed", e);
        }
    }
}