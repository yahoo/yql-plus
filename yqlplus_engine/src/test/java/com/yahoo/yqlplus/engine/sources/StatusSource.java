/*
 * Copyright (c) 2017 Yahoo Holdings
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Set;

public class StatusSource implements Source {
    @Insert
    public BulkResponse addBalance(@Set("payload") String payload)  {
        List<? extends ResponseObj> bulkResponseItems = new ArrayList<>();
        MoreDetailedResponse item = new MoreDetailedResponse();
        item.setErrorCode("500");
        item.setId("id");
        item.setStatus(true);
        item.setAdditinalId("realizeTxnId");
        List<MoreDetailedResponse> items = new ArrayList<>();
        items.add(item);
        bulkResponseItems = items;
        BulkResponse response = new BulkResponse(bulkResponseItems);
        return response;
    }
}
