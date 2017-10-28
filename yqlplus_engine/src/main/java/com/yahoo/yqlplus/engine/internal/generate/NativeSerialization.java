/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.fasterxml.jackson.core.JsonGenerator;

public interface NativeSerialization {
    void writeJson(JsonGenerator target, Object input);
}
