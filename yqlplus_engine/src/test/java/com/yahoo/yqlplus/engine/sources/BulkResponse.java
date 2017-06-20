/*
 * Copyright (c) 2017 Yahoo Holdings
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.ArrayList;
import java.util.List;

public class BulkResponse {

    List<? extends ResponseObj> responseItems;


    public BulkResponse() {
        responseItems = new ArrayList<>();
    }

    public BulkResponse(List<? extends ResponseObj> items) {
        responseItems = items;
    }

    public String getPayload() {
        return "";
    }

    public List<? extends ResponseObj> getBulkResponseItems() {
        return responseItems;
    }

    public void setBulkResponseItems(List<ResponseObj> bulkResponseItems) {
        this.responseItems = bulkResponseItems;
    }

    public void setResponses(List<? extends ResponseObj> items) {
        responseItems = items;
    }

    @Override
    public String toString() {
      return "BulkResponse [bulkResponseItems=" + responseItems + "]";
    }
}
