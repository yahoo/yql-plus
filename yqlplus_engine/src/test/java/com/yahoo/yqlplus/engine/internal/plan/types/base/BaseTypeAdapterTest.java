/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BaseTypeAdapterTest {
    private BaseTypeAdapter adapter;

    @BeforeClass
    public void setUp() {
        this.adapter = new BaseTypeAdapter();
    }

    @Test
    public void testName() {


    }
}
