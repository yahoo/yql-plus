/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.network.api;

import com.yahoo.yqlplus.api.types.YQLType;

import java.util.List;


public interface DataSourceDescriptor {
    List<String> getName();

    YQLType getRowType();
}
