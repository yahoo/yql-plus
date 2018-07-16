/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.sample;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.engine.*;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Standing in for an engine container.
 */
public class Container implements EngineContainerInterface {
    static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();


    public Map<String, JsonNode> run(String script, Object... bindings) throws Exception {
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        YQLPlusCompiler compiler = YQLPlusEngine.builder().bind(bindings).build();
        CompiledProgram program = compiler.compile(script);
        //program.dump(System.err);
        ProgramResult result = program.run(Maps.newHashMap());
        try {
            result.getEnd().get(10000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Map<String, JsonNode> parsed = Maps.newLinkedHashMap();
        for (String key : result.getResultNames()) {
            YQLResultSet data = result.getResult(key).get();
            Object rez = data.getResult();
            ByteArrayOutputStream outstream = new ByteArrayOutputStream();
            JsonGenerator gen = JSON_FACTORY.createGenerator(outstream);
            gen.writeObject(rez);
            gen.flush();
            parsed.put(key, JSON_FACTORY.createParser(outstream.toByteArray()).readValueAsTree());
        }
        return parsed;
    }

}
