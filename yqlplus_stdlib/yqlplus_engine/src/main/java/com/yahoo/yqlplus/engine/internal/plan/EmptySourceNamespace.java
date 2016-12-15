/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.util.List;

public class EmptySourceNamespace implements SourceNamespace {
    @Override
    public SourceType findSource(Location location, ContextPlanner planner, List<String> path) {
        throw new ProgramCompileException(location, "Empty source namespace can't bind");
    }
}
