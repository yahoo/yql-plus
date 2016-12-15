/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.network.api;

import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

/**
 * Represents a service which is publishing one or more sources (tables).
 */
public interface DataService {
    List<DataSourceDescriptor> getSources();

    List<ProgramDescriptor> getPrograms();
    // TODO: maybe represent the UDFs available via this service to know which UDF invocations may be pushed "down"

    // TODO: would like a way to negotiate/represent better support for few/zero-copy of serialization data

    PreparedInvocation prepareInvocation(OperatorNode<StatementOperator> program);

    PreparedInvocation prepareCall(String programName);
}
