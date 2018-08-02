/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import com.google.common.base.Ticker;

public final class RelativeTicker extends Ticker {
    private final long start;
    private final Ticker base;

    public RelativeTicker(Ticker base) {
        this.base = base;
        this.start = base.read();
    }

    @Override
    public long read() {
        return base.read() - start;
    }

    RelativeTicker child() {
        return new RelativeTicker(base);
    }
}
