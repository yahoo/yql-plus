/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.guice;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.yahoo.yqlplus.engine.compiler.code.TypeAdaptingWidget;

public class ASMClassSourceModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<TypeAdaptingWidget> binder = Multibinder.newSetBinder(binder(), TypeAdaptingWidget.class);
    }
}
