/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;


import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.io.IOException;

public interface ProgramCompiler {
    CompiledProgram compile(OperatorNode<StatementOperator> program) throws IOException;
}
