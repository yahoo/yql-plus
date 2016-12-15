/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.network.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface PreparedInvocation {
    ProgramDescriptor getDescriptor();

    void invoke(long timeout, TimeUnit timeoutUnits, Map<String, Object> arguments, InvocationHandler output);
}
