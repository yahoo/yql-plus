/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.*;
import com.yahoo.yqlplus.engine.api.Namespace;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Several ways to tell the engine about sources.
 * <p/>
 * This does not use the JavaEngineModule but instead composes the engine using the smaller modules to control how
 * dependencies get resolved.
 */
public class NamespaceBindingsTest {
    public static final class MySource implements Source {
        @Query
        public Iterable<Person> scan() {
            return ImmutableList.of(new Person("1", "1", 1));
        }
    }



    @Test
    public void testGuicelessEntryPoint() throws Exception {
        YQLPlusCompiler compiler = YQLPlusEngine.createCompiler(new Namespace() {
            @Override
            public Provider<Source> resolveSource(List<String> path) {
                if (path.size() == 1 && path.get(0).equals("mine")) {
                    return new Provider<Source>() {
                        @Override
                        public Source get() {
                            return new MySource();
                        }
                    };
                }
                return null;
            }

            @Override
            public Provider<Exports> resolveModule(List<String> path) {
                return null;
            }

            @Override
            public OperatorNode<SequenceOperator> getView(List<String> name) {
                return null;
            }
        });
        runInvocations(compiler);
    }

    // run the same sequence of queries against the given engine
    private void runInvocations(YQLPlusCompiler compiler) throws Exception {
        CompiledProgram program = compiler.compile("SELECT * FROM mine OUTPUT AS f1;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("f1").get();
        Assert.assertEquals(rez.getResult(), ImmutableList.of(new Person("1", "1", 1)));

        // now verify it reports missing sources properly
        try {
            program = compiler.compile("SELECT * FROM missing OUTPUT AS f1;");
            Assert.fail("Should not reach here - should have thrown a ProgramCompileException");
        } catch (ProgramCompileException e) {
            Assert.assertEquals(e.getMessage(), "<string>:L1:14 No source 'missing' found");
        }
    }
}
