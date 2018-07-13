/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.compiler.code.TypeAdaptingWidget;
import com.yahoo.yqlplus.engine.internal.compiler.PlanProgramCompiler;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Transform CQL programs into executable. Permit introspection.
 */
@Singleton
public class YQLPlusCompiler {
    private final PlanProgramCompiler compiler;

    @Inject
    public YQLPlusCompiler(Set<TypeAdaptingWidget> adapters, SourceNamespace sourceNamespace, ModuleNamespace moduleNamespace, ViewRegistry viewNamespace) {
        this.compiler = new PlanProgramCompiler(adapters, sourceNamespace, moduleNamespace, viewNamespace);
    }


    public CompiledProgram compile(final String programName, final InputStream stream) throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        CompiledProgram compiledProgram = compile(parser.parse(programName, stream));
        return compiledProgram;
    }

    public CompiledProgram compile(final String programName, final String program) throws RecognitionException, IOException {
        ProgramParser parser = new ProgramParser();
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
        return compiler.compile(program);
    }
}
