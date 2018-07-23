package com.yahoo.yqlplus.integration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLPlusEngine;
import com.yahoo.yqlplus.engine.YQLResultSet;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class MovieServer {
    public static void main(final String[] args) throws IOException {
        YQLPlusCompiler compiler = YQLPlusEngine.builder()
        .bind(
                "movies", new MovieSource(new Movie("1", "joe", "drama", "2017-01-01", 10))
        ).build();
        JsonFactory factory = new MappingJsonFactory();
        CompiledProgram program = compiler.compile("SELECT * FROM movies OUTPUT AS movies;");
        Undertow server = Undertow.builder()
                .addHttpListener(8080, "localhost")
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(final HttpServerExchange exchange) throws Exception {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                        if("/movies".equals(exchange.getRequestPath())) {
                            ProgramResult myResult = program.run(ImmutableMap.of());
                            YQLResultSet rez = myResult.getResult("foo").get();
                            List<Movie> foo = rez.getResult();
                            exchange.setStatusCode(200);
                            OutputStream output = exchange.getOutputStream();
                            JsonGenerator out = factory.createGenerator(output);
                            out.writeObject(foo);
                            output.close();
                            return;
                        }
                        exchange.setStatusCode(404);
                        OutputStream output = exchange.getOutputStream();
                        JsonGenerator out = factory.createGenerator(output);
                        out.writeString("Not found");
                    }
                }).build();
        server.start();
    }}
