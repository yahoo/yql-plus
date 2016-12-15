/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.java.backends.java;

import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public interface QueryMethod {
	TypeWidget getRecordType();

    String getKeyName();

    TypeWidget getKeyType();
}
