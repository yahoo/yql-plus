/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.network.api;

public interface InvocationHandler {
    public enum ResultCode {
        FAIL,
        TRANSIENT,
        BUSY
    }

    // fail entire program
    void fail(Throwable t);

    void fail(ResultCode code, String message);

    // succeed a resultset: value MUST conform to both the YQLType and the JVMType exposed by the ProgramDescriptor
    void succeedResult(String name, Object value);

    void failResult(String name, Throwable t);

    void failResult(String name, ResultCode code, String message);

    void end();
}
