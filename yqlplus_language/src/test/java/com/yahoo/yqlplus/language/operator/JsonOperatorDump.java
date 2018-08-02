/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.operator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public class JsonOperatorDump {
    static class ProgramDumpModule extends SimpleModule {
        public ProgramDumpModule() {
            addSerializer(OperatorNode.class, new JsonSerializer<OperatorNode>() {
                @Override
                public void serialize(OperatorNode value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
                    Operator op = value.getOperator();
                    jgen.writeStartObject();
                    if (value.getLocation() != null) {
                        jgen.writeArrayFieldStart("location");
                        jgen.writeNumber(value.getLocation().getLineNumber());
                        jgen.writeNumber(value.getLocation().getCharacterOffset());
                        jgen.writeEndArray();
                    }
                    jgen.writeStringField("type", op.getClass().getName());
                    jgen.writeStringField("operator", op.toString());
                    if (!value.getAnnotations().isEmpty()) {
                        jgen.writeObjectField("annotations", value.getAnnotations());
                    }
                    jgen.writeFieldName("arguments");
                    provider.defaultSerializeValue(value.getArguments(), jgen);
                    jgen.writeEndObject();
                }
            });
        }
    }

    public String dump(OperatorNode<? extends Operator> program) throws IOException {
        MappingJsonFactory factory = new MappingJsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        mapper.registerModule(new ProgramDumpModule());
        return mapper.writer().withDefaultPrettyPrinter().writeValueAsString(program);
    }
}
