/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.tools;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.yahoo.yqlplus.api.trace.TraceRequest;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.guice.JavaEngineModule;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Run a CQL script from the command line.  Prints the results.
 */
public class YQLPlusRun {
    private JsonFactory factory = new MappingJsonFactory();

    protected Options createOptions() {
        Options options = new Options();
        options.addOption(new Option("h", "help", false, "Display help"));
        options.addOption(new Option("c", "command", true, "Execute a script from the command line"));
        options.addOption(new Option("s", "source", false, "Dump source of generated script"));
        options.addOption(new Option("v", "verbose", false, "Enable verbose tracing"));
        options.addOption(new Option("p", "path", true, "Add a module directory"));

        // -l <port> (listen on a port? for dev-time running of a web server)
        // a way to import a library?

        // command -h -Dargname=argval -Dargname=argvalue... -c {command} [<file>]

        options.addOption(OptionBuilder.withArgName("argument=value").hasArgs(2).withValueSeparator().withDescription("define named argument").create("D"));
        return options;
    }

    public final int run(String[] strings) throws Exception {
        Options options = createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine command;
        try {
            command = parser.parse(options, strings);
        } catch (ParseException e) {
            System.err.println("Command line parse exception: " + e.getMessage());
            return -1;
        }
        if (command.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getClass().getName(), options);
            return 0;
        }
//        LogManager manager = LogManager.getLogManager();
//        String logLevel = command.getOptionValue("loglevel", "WARNING");
//        Logger logger = Logger.getGlobal();
//        logger.setLevel(Level.parse(logLevel));
        return run(command);
    }

    @SuppressWarnings("unchecked")
    public int run(CommandLine command) throws Exception {
        String script = null;
        String filename = null;
        if (command.hasOption("command")) {
            script = command.getOptionValue("command");
            filename = "<command line>";
        }
        List<String> scriptAndArgs = (List<String>) command.getArgList();
        if (filename == null && scriptAndArgs.size() < 1) {
            System.err.println("No script specified.");
            return -1;
        } else if (script == null) {
            filename = scriptAndArgs.get(0);
            Path scriptPath = Paths.get(filename);
            if (!Files.isRegularFile(scriptPath)) {
                System.err.println(scriptPath + " is not a file.");
                return -1;
            }
            script = Charsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(scriptPath))).toString();
        }
        List<String> paths = Lists.newArrayList();
        if (command.hasOption("path")) {
            paths.addAll(Arrays.asList(command.getOptionValues("path")));
        }
        // TODO: this isn't going to be very interesting without some sources
        Injector injector = Guice.createInjector(new JavaEngineModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(script);
        if (command.hasOption("source")) {
            program.dump(System.out);
            return 0;
        }
        // TODO: read command line arguments to pass to program
        ExecutorService outputThreads = Executors.newSingleThreadExecutor();
        ProgramResult result = program.run(Maps.<String, Object>newHashMap(), true);
        for (String name : result.getResultNames()) {
            final ListenableFuture<YQLResultSet> future = result.getResult(name);
            future.addListener(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                YQLResultSet resultSet = future.get();
                                System.out.println(new String((byte[]) resultSet.getResult()));
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }

                        }
                    }, outputThreads);
        }
        Future<TraceRequest> done = result.getEnd();
        try {
            done.get(10000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }
        outputThreads.awaitTermination(1L, TimeUnit.SECONDS);
        return 0;
    }

    private static class ModulePathModule extends AbstractModule {
        private List<String> paths;

        private ModulePathModule(List<String> paths) {
            this.paths = paths;
        }

        @Override
        protected void configure() {
            Multibinder<String> mbinder = Multibinder.newSetBinder(binder(), String.class, Names.named("modulePath"));
            for (String path : paths) {
                mbinder.addBinding().toInstance(path);
            }
        }
    }

    @Subscribe
    public void event(Object event) throws IOException {
        JsonGenerator gen = factory.createGenerator(System.err);
        gen.writeStartObject();
        gen.writeStringField("type", event.getClass().getName());
        gen.writeFieldName("event");
        gen.writeObject(event);
        gen.writeEndObject();
        gen.flush();
        System.err.println();
    }

    public static void main(String[] args) throws Exception {
        System.exit(new YQLPlusRun().run(args));
    }
}
