package com.yahoo.yqlplus.integration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLPlusEngine;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.integration.sources.Movie;
import com.yahoo.yqlplus.integration.sources.MovieSource;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class MovieServer {
    public static void main(final String[] args) throws IOException {
        ForkJoinPool pool = ForkJoinPool.commonPool();
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
                        if (exchange.isInIoThread()) {
                            exchange.dispatch(this);
                            return;
                        }
                        try {
                            if ("/movies".equals(exchange.getRequestPath())) {
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                TaskContext context = TaskContext.builder()
                                        .withPool(pool)
                                        .build();
                                ProgramResult myResult = program.run(ImmutableMap.of(), context);
                                YQLResultSet rez = myResult.getResult("movies").get();
                                List<Movie> foo = rez.getResult();
                                exchange.startBlocking();
                                OutputStream output = exchange.getOutputStream();
                                JsonGenerator out = factory.createGenerator(output);
                                out.writeObject(foo);
                                out.flush();
                                out.close();

                                return;
                            }
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                            exchange.setStatusCode(404);
                            exchange.getResponseSender().send("Not found");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).build();
        server.start();

    }
}
