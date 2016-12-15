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
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.yahoo.cloud.metrics.api.*;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.guice.JavaEngineModule;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Standing in for an engine container.
 */
public class Container implements EngineContainerInterface {
    static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();


    public Map<String, JsonNode> run(String script, final Module... modules) throws Exception {
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        Injector injector = Guice.createInjector(new JavaEngineModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ViewRegistry.class).toInstance(new ViewRegistry() {
                            @Override
                            public OperatorNode<SequenceOperator> getView(List<String> name) {
                                return null;
                            }
                        });
                        bind(TaskMetricEmitter.class).toInstance(new StandardRequestEmitter(new MetricDimension(), new RequestMetricSink() {
                            @Override
                            public void emitRequest(RequestEvent arg0) {                              
                            }
                        }).start(new MetricDimension()));                      
                    }            
                },
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        for (Module module : modules) {
                            install(module);
                        }
                    }
                });
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(script);
        //program.dump(System.err);
        ProgramResult result = program.run(Maps.<String, Object>newHashMap(), true);
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
            parsed.put(key, (JsonNode) JSON_FACTORY.createParser(outstream.toByteArray()).readValueAsTree());
        }
        return parsed;
    }

}
