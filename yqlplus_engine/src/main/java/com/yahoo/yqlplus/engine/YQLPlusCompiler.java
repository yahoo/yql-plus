/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.engine.internal.scope.ExecutionScoper;
import com.yahoo.yqlplus.engine.scope.EmptyExecutionScope;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;
import com.yahoo.yqlplus.engine.scope.WrapScope;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * Transform CQL programs into executable. Permit introspection.
 */
@Singleton
public class YQLPlusCompiler {
    private final Provider<ProgramCompiler> provider;
    private final Provider<ProgramParser> transformer;
    private final ExecutionScoper scoper;

    @Inject(optional = true)
    @Named("compile")
    private ExecutionScope compileExecutionScope =
            new WrapScope(new EmptyExecutionScope())
                    .bind(Tracer.class, new DummyTracer());


    @Inject
    YQLPlusCompiler(Provider<ProgramCompiler> provider, Provider<ProgramParser> transformer, ExecutionScoper scoper) {
        this.provider = provider;
        this.transformer = transformer;
        this.scoper = scoper;
    }

    private ExecutionScope createCompileExecutionScope(String programName) {
        return new WrapScope(compileExecutionScope)
                .bind(Boolean.class, "debug", true)
                .bind(String.class, "programName", programName);
    }

    public CompiledProgram compile(final String programName, final InputStream stream) throws IOException, RecognitionException {
        try {
            return scoper.startScope(new Callable<CompiledProgram>() {
                @Override
                public CompiledProgram call() throws Exception {
                    ProgramParser parser = transformer.get();
                    CompiledProgram compiledProgram = compile(parser.parse(programName, stream));
                    return compiledProgram;
                }
            }, createCompileExecutionScope(programName)).call();
        } catch (RuntimeException | IOException e) {
            throw e;
        } catch (Exception e) {
            // this won't really happen, because the underlying call only throws the above
            throw new ProgramCompileException(e);
        }
    }

    public CompiledProgram compile(final String programName, final String program) throws RecognitionException, IOException {
        try {
            return scoper.startScope(new Callable<CompiledProgram>() {
                @Override
                public CompiledProgram call() throws Exception {
                    ProgramParser parser = transformer.get();
                    CompiledProgram compiledProgram = compile(parser.parse(programName, program));
                    return compiledProgram;
                }
            }, createCompileExecutionScope(programName)).call();
        } catch (RuntimeException | IOException e) {
            throw e;
        } catch (Exception e) {
            // this won't really happen, because the underlying call only throws the above
            throw new ProgramCompileException(e);
        }
    }

    public CompiledProgram compile(final InputStream stream) throws IOException, RecognitionException {
        return compile("<stream>", stream);
    }

    public CompiledProgram compile(final String program) throws RecognitionException, IOException {
        return compile("<string>", program);
    }

    private CompiledProgram compile(OperatorNode<StatementOperator> program) throws IOException {
        ProgramCompiler programBuilder = provider.get();
        return programBuilder.compile(program);
    }
}
