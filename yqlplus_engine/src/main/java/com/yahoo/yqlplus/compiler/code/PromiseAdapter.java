/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.yahoo.yqlplus.compiler.generate.ScopedBuilder;

public interface PromiseAdapter {
    TypeWidget getResultType();

    //    public interface PromiseChain extends ScopedBuilder {
//        BytecodeExpression getIsSuccess();
//        BytecodeExpression getFailure();
//        BytecodeExpression getResult();
//        BytecodeExpression resolve();
//
//        BytecodeExpression build();
//    }
//
    BytecodeExpression resolve(ScopedBuilder scope, BytecodeExpression timeout, BytecodeExpression target);
}

