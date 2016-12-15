/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.rules;

import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

@Test
public class LogicalProgramTransformsTest {
    @Test
    public void requireSubqueryReplacement() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("program.yql",
                "SELECT * " +
                        "  FROM source1 WHERE id IN (SELECT id2 FROM source2);"
        );
        LogicalProgramTransforms transforms = new LogicalProgramTransforms();
        OperatorNode<StatementOperator> operator = transforms.apply(program, new ViewRegistry() {
            @Override
            public OperatorNode<SequenceOperator> getView(List<String> name) {
                return null;
            }
        });
        Assert.assertEquals(operator.toString(), "(PROGRAM L0:0 [(EXECUTE (EXTRACT {rowType=[source2]} (SCAN L53:1 {alias=source2, rowType=[source2]} [source2], []), (READ_FIELD L44:1 {rowType=[source2]} source2, id2)), subquery$0), (EXECUTE L0:1 (FILTER {rowType=[source1]} (SCAN L16:1 {alias=source1, rowType=[source1]} [source1], []), (IN (READ_FIELD L30:1 {rowType=[source1]} source1, id), (VARREF subquery$0))), result1), (OUTPUT L0:1 result1)])");
    }

    @Test
    public void requireSubSubqueryReplacement() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("program.yql",
                "SELECT * " +
                        "  FROM source1 WHERE id IN (SELECT id2 FROM source2 WHERE id2 IN (SELECT a + 1 FROM source3));"
        );
        LogicalProgramTransforms transforms = new LogicalProgramTransforms();
        OperatorNode<StatementOperator> operator = transforms.apply(program, new ViewRegistry() {
            @Override
            public OperatorNode<SequenceOperator> getView(List<String> name) {
                return null;
            }
        });
        Assert.assertEquals(operator.toString(), "(PROGRAM L0:0 [(EXECUTE (EXTRACT {rowType=[source3]} (SCAN L93:1 {alias=source3, rowType=[source3]} [source3], []), (ADD (READ_FIELD L82:1 {rowType=[source3]} source3, a), (LITERAL L86:1 1))), subquery$0), (EXECUTE (EXTRACT {rowType=[source2]} (FILTER {rowType=[source2]} (SCAN L53:1 {alias=source2, rowType=[source2]} [source2], []), (IN (READ_FIELD L67:1 {rowType=[source2]} source2, id2), (VARREF subquery$0))), (READ_FIELD L44:1 {rowType=[source2]} source2, id2)), subquery$1), (EXECUTE L0:1 (FILTER {rowType=[source1]} (SCAN L16:1 {alias=source1, rowType=[source1]} [source1], []), (IN (READ_FIELD L30:1 {rowType=[source1]} source1, id), (VARREF subquery$1))), result1), (OUTPUT L0:1 result1)])");
    }
}
