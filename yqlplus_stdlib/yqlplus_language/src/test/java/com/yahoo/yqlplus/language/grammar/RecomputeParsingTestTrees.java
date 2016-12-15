/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.grammar;

import org.antlr.v4.runtime.RecognitionException;

import java.io.File;
import java.io.IOException;

/**
 * Recompute the test expected results used by TestParsing
 */
public class RecomputeParsingTestTrees {
    public static void main(String[] args) throws IOException, RecognitionException {
        // recompute the parse trees
        File target = new File("yqlplus_language/src/test/resources/com/yahoo/yqlplus/language/grammar/trees.txt");
        TestParsing parser = new TestParsing();
        File output = parser.recomputeParseTree(target);
        target.delete();
        output.renameTo(target);
        System.out.println("Recomputed test trees");
    }

}
