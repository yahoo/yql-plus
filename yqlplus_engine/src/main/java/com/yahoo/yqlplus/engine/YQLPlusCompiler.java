/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Transform CQL programs into executable. Permit introspection.
 */
@Singleton
public class YQLPlusCompiler {
    private final Provider<ProgramCompiler> provider;
    private final Provider<ProgramParser> transformer;



    @Inject
    YQLPlusCompiler(Provider<ProgramCompiler> provider, Provider<ProgramParser> transformer) {
        this.provider = provider;
        this.transformer = transformer;
    }


    public CompiledProgram compile(final String programName, final InputStream stream) throws IOException, RecognitionException {
        ProgramParser parser = transformer.get();
        CompiledProgram compiledProgram = compile(parser.parse(programName, stream));
        return compiledProgram;
    }

    public CompiledProgram compile(final String programName, final String program) throws RecognitionException, IOException {
        ProgramParser parser = transformer.get();
        CompiledProgram compiledProgram = compile(parser.parse(programName, program));
        return compiledProgram;
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
