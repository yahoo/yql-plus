/*
 * Copyright (c) 2017 Yahoo Holdings
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

public class MoreDetailedResponse extends ResponseObj {

    private String additionalId;

    public String getAdditionalId() {
        return additionalId;
    }

    public MoreDetailedResponse setAdditinalId(String additionalId) {
        this.additionalId = additionalId;
        return this;
    }
}
