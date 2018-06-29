/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.api.NativeInvocationResultHandler;
import com.yahoo.yqlplus.engine.scope.ExecutionScope;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A CompiledProgram represents a ready-to-execute program. It has methods to inspect its input (arguments) and output
 * (resultsets).
 */
public interface CompiledProgram {

    enum ProgramStatement {
        SELECT, INSERT, UPDATE, DELETE
    }

    interface ArgumentInfo {
        String getName();

        Type getType();

        boolean isRequired();

        YQLType getYQLType();
        
        Object getDefaultValue();
    }

    interface ResultSetInfo {
        String getName();

        /**
         * Gives the Type of this result set -- for example, a List&lt;SomeRecordtype&gt;
         */
        Type getResultType();
    }

    /**
     * Invoke the compiled program.
     */
    ProgramResult run(Map<String, Object> arguments, boolean debug) throws Exception;
    
    /**
     * Invoke the compiled program with explicit timeout
     */
    ProgramResult run(Map<String, Object> arguments, boolean debug, long timeout, TimeUnit timeoutUnit) throws Exception;
    /**
     * Invoke the compiled program within the given ExecutionScope.
     *
     * @see ExecutionScope
     */
    ProgramResult run(Map<String, Object> arguments, boolean debug, ExecutionScope scope) throws Exception;
    
    ProgramResult run(Map<String, Object> arguments, boolean debug, ExecutionScope scope, long timeout, TimeUnit timeoutUnit) throws Exception;

    void invoke(InvocationResultHandler resultHandler, Map<String, Object> arguments, ExecutionScope scope, TaskContext context);

    void invoke(NativeEncoding encoding, NativeInvocationResultHandler resultHandler, Map<String, Object> arguments, ExecutionScope scope, TaskContext context);

    /**
     * Enumerated the arguments for this program. Required arguments must be passed to run to invoke the program.
     */
    List<ArgumentInfo> getArguments();

    /**
     * Enumerate the resultset names and types which will be produced by this program.
     */
    List<ResultSetInfo> getResultSets();

    /**
     * Dumps a representation of the compiled program for inspection and debugging.
     *
     * @throws IOException
     */
    void dump(OutputStream out) throws IOException;

    /**
     * Provides access to named views defined by the program.
     */
    OperatorNode<SequenceOperator> getView(String name);

    /**
     * Enumerates the names of views defined by this program.
     */
    List<String> getViewNames();

    /**
     * Checks if this CompiledProgram contains the given program statement.
     *
     * @param programStatement
     * @return
     */
    boolean containsStatement(ProgramStatement programStatement);
}
